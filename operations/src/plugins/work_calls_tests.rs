//! Tests the checks of a work attempt and the job reads as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::PluginWorkAction;

use super::*;
use crate::plugins::rows::tests::{child_result, values_result};
use crate::plugins::work::WorkKind;
use crate::plugins::work::tests::sample_install;

fn spec(install: &PluginInstall) -> PluginWorkSpec {
    PluginWorkSpec {
        realm_id: install.account.realm_id,
        install_id: install.install_id,
        work_id: Ulid::from_parts(2, 2),
        group_id: install.group_id,
        action: PluginWorkAction::Copy {
            source: "/s".into(),
            source_version: None,
            target: "/t".into(),
            version_id: Ulid::from_parts(3, 3),
        },
    }
}

fn config_of(install: &PluginInstall) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(install.account.realm_id, Vec::new(), 3);
    config.installs.push(install.clone());
    config
}

fn row_of(install: &PluginInstall) -> WorkRow {
    WorkRow::reserved(install, WorkKind::Copy, Ulid::nil(), 1)
}

fn is_child<T>(next: Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

#[test]
fn admission_reads_in_order() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut admit = AdmitWork::new(&spec(&install), install.receiver);
    assert!(is_child(admit.first().unwrap()));
    let config = child_result(Ok::<_, GetConfigError>(config_of(&install)));
    assert!(is_child(admit.next(config).unwrap()));
    let row = child_result(Ok::<_, StoreError>(Some(row_of(&install))));
    assert!(matches!(admit.next(row), Ok(Next::Call(IoCall::Read(_)))));
    // No account row: the check of the install refuses.
    let refused = admit.next(values_result(vec![None])).unwrap_err();
    assert!(matches!(refused, WorkRefusal::Refused(_)));
}

#[test]
fn forgotten_work_ends() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut admit = AdmitWork::new(&spec(&install), install.receiver);
    let config = child_result(Ok::<_, GetConfigError>(config_of(&install)));
    admit.next(config).unwrap();
    let gone = child_result(Ok::<Option<WorkRow>, StoreError>(None));
    let refused = admit.next(gone).unwrap_err();
    assert!(matches!(refused, WorkRefusal::Permanent(_)));
}

#[test]
fn unread_config_defers() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut admit = AdmitWork::new(&spec(&install), install.receiver);
    let missing = child_result(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    let refused = admit.next(missing).unwrap_err();
    assert!(matches!(refused, WorkRefusal::Deferred(_)));
}

#[test]
fn transfer_needs_request() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let row = row_of(&install);
    let mut grants = TransferGrants::download(&install, &row, "/t");
    assert_eq!(grants.first().unwrap_err().0.kind, ErrorKind::Invalid);
}

#[test]
fn transfer_ends_with_patterns() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut grants = TransferGrants::upload(&install, &row_of(&install), "/s");
    let patterns = BTreeSet::from(["/s".to_string()]);
    grants.patterns = Some(patterns.clone());
    let passed = child_result(Ok::<(), CallError>(()));
    assert_eq!(grants.next(passed), Ok(Next::Done(patterns)));
}

#[test]
fn job_row_of_install() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let row = row_of(&install);
    let id = row.work_id.to_string();
    let mut read = JobRow::new(&install, &id);
    assert!(is_child(read.first().unwrap()));
    let found = child_result(Ok::<_, StoreError>(Some(row.clone())));
    assert_eq!(read.next(found), Ok(Next::Done(row.clone())));
    let mut earlier = row;
    earlier.account = UserId::local(Ulid::from_parts(7, 7), install.account.realm_id);
    let mut read = JobRow::new(&install, &id);
    let other = child_result(Ok::<_, StoreError>(Some(earlier)));
    assert_eq!(read.next(other).unwrap_err().0.kind, ErrorKind::NotFound);
    let mut bad = JobRow::new(&install, "x");
    assert_eq!(bad.first().unwrap_err().0.kind, ErrorKind::NotFound);
}

#[test]
fn outputs_without_summary() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let place = (&install, install.receiver);
    let mut outputs = JobOutputs::new(place, "id", None);
    assert!(is_child(outputs.first().unwrap()));
    let row = child_result(Ok::<_, CallError>(row_of(&install)));
    let expected = ObjectPage {
        objects: Vec::new(),
        cursor: None,
    };
    assert_eq!(outputs.next(row), Ok(Next::Done(expected)));
    let mut bad = JobOutputs::new(place, "id", Some("x".into()));
    let row = child_result(Ok::<_, CallError>(row_of(&install)));
    assert_eq!(bad.next(row).unwrap_err().0.kind, ErrorKind::Invalid);
}
