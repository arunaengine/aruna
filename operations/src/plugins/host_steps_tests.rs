//! Tests the steps of a run's host calls as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::errors::StorageError;
use serde_json::json;

use super::*;
use crate::jobs::repository::test_support::registry_record;
use crate::metadata::api::list_types::ListedMetadataDocument;
use crate::plugins::fixtures::{self, config_result, is_child, node};
use crate::plugins::rows::tests::child_result;
use crate::plugins::runs::tests::queued;
use crate::plugins::submit::CopyRequest;

fn meta_root() -> String {
    let install = fixtures::install();
    format!("/{}/g/{}/meta", fixtures::realm(), install.group_id)
}

fn install() -> PluginInstall {
    let mut install = fixtures::install();
    let whole = vec![format!("{}/**", meta_root())];
    install.grants = BTreeMap::from([
        ("read".to_string(), whole.clone()),
        ("write".to_string(), whole),
    ]);
    install
}

fn scope() -> RunScope {
    RunScope {
        realm_id: fixtures::realm(),
        local: node(3),
        run: queued(install().install_id, 1),
    }
}

fn place() -> ReadPlace {
    ReadPlace {
        scope: scope(),
        network_node: Some(node(3)),
        max_bytes: 1024,
    }
}

fn intent() -> CallIntent {
    CallIntent {
        ordinal: 0,
        digest: [1; 32],
    }
}

fn checked_install() -> IoResult {
    child_result(Ok::<_, CallError>(install()))
}

fn access() -> Granted {
    Granted {
        install: install(),
        auth: fixtures::caller().auth,
    }
}

fn granted_result() -> IoResult {
    child_result(Ok::<_, CallError>(access()))
}

fn record() -> MetadataRegistryRecord {
    let mut record = registry_record();
    record.permission_path = format!("{}/doc", meta_root());
    record
}

fn resource() -> String {
    Ulid::from_parts(4, 4).to_string()
}

fn summary(jsonld: &str) -> IoResult {
    child_result(Ok::<_, MetadataApiError>(ExportMetadataResult::Summary {
        record: record(),
        jsonld: jsonld.to_string(),
    }))
}

fn refusal() -> CallError {
    CallError(HostError::new(ErrorKind::Denied, "no"))
}

fn fresh() -> IoResult {
    child_result(Ok::<_, StoreError>(Begun::Fresh))
}

#[test]
fn admit_passes_install() {
    let mut admit = AdmitCall::new(&scope(), None);
    assert!(is_child(&admit.first()));
    assert_eq!(admit.next(checked_install()), Ok(Next::Done(install())));
    let mut refused = AdmitCall::new(&scope(), None);
    refused.first().unwrap();
    let answer = child_result(Err::<PluginInstall, _>(refusal()));
    assert_eq!(refused.next(answer), Err(refusal()));
}

#[test]
fn granted_restricts_account() {
    let target = format!("{}/doc", meta_root());
    let mut call = GrantedCall::target(&scope(), Permission::MetadataRead, &target);
    assert!(is_child(&call.first()));
    let Ok(Next::Done(access)) = call.next(checked_install()) else {
        panic!("a covered target is granted");
    };
    assert!(access.auth.path_restrictions.is_some());
    let other = "/elsewhere";
    let mut call = GrantedCall::target(&scope(), Permission::MetadataRead, other);
    call.first().unwrap();
    assert!(call.next(checked_install()).is_err());
}

#[test]
fn read_refuses_resource() {
    let view = RoCrateExportView::Full;
    let mut read = ReadDataset::new(place(), "not an id", view);
    assert!(read.first().is_err());
}

#[test]
fn read_checks_path() {
    let jsonld = json!({"@graph": [{"@id": "ro-crate-metadata.json"}, {"@id": "./"}]});
    let view = RoCrateExportView::Summary;
    let mut read = ReadDataset::new(place(), &resource(), view);
    assert!(is_child(&read.first()));
    assert!(is_child(&read.next(granted_result())));
    assert!(is_child(&read.next(summary(&jsonld.to_string()))));
    let denied = child_result(Err::<Granted, _>(refusal()));
    assert_eq!(read.next(denied).unwrap_err(), refusal());
}

#[test]
fn files_need_page() {
    let mut read = ReadFiles::new(place(), &resource(), None);
    read.first().unwrap();
    read.next(granted_result()).unwrap();
    let Err(CallError(error)) = read.next(summary("{}")) else {
        panic!("a whole export is no page");
    };
    assert_eq!(error.kind, ErrorKind::Unavailable);
}

fn listed(paths: &[String]) -> IoResult {
    let documents: Vec<_> = (paths.iter())
        .map(|path| {
            let mut record = record();
            record.permission_path = path.clone();
            ListedMetadataDocument {
                record,
                rocrate_summary_jsonld: None,
            }
        })
        .collect();
    let result = ListVisibleResult {
        total_returned: documents.len(),
        documents,
        limit: LIST_PAGE,
        offset: 0,
        total_estimate: None,
    };
    child_result(Ok::<_, MetadataApiError>(result))
}

#[test]
fn list_keeps_authorized() {
    let covered = format!("{}/doc", meta_root());
    let mut list = ListDatasets::new(&scope(), None);
    assert!(is_child(&list.first()));
    assert!(is_child(&list.next(granted_result())));
    let answer = listed(&["/outside".to_string(), covered]);
    assert!(is_child(&list.next(answer)));
    let Ok(Next::Done(listing)) = list.next(child_result(Ok::<(), CallError>(()))) else {
        panic!("the last check ends the page");
    };
    assert_eq!(listing.resources.len(), 1);
    assert_eq!(listing.cursor, None);
}

#[test]
fn list_refuses_cursor() {
    let mut list = ListDatasets::new(&scope(), Some("x".to_string()));
    list.first().unwrap();
    assert!(list.next(granted_result()).is_err());
}

#[test]
fn list_stops_unavailable() {
    let mut list = ListDatasets::new(&scope(), None);
    list.first().unwrap();
    list.next(granted_result()).unwrap();
    list.next(listed(&[format!("{}/doc", meta_root())]))
        .unwrap();
    let down = CallError(unavailable("down"));
    let answer = child_result(Err::<(), _>(down.clone()));
    assert_eq!(list.next(answer).unwrap_err(), down);
}

fn object_path() -> String {
    format!("/{}/g/{}/data/a", fixtures::realm(), install().group_id)
}

fn hashed(hash: Option<[u8; 32]>) -> CheckObjects {
    let named = NamedObjects {
        objects: vec![(object_path(), None)],
        hash,
    };
    CheckObjects::new(&scope(), &install(), named)
}

fn located() -> IoResult {
    child_result(Ok::<_, CallError>(("b".to_string(), "k".to_string())))
}

#[test]
fn objects_pass_checks() {
    let mut check = hashed(None);
    assert!(is_child(&check.first()));
    assert!(is_child(&check.next(granted_result())));
    assert_eq!(check.next(located()), Ok(Next::Done(())));
}

#[test]
fn hash_needs_object() {
    let mut check = hashed(Some([1; 32]));
    check.first().unwrap();
    check.next(granted_result()).unwrap();
    assert!(is_child(&check.next(located())));
    let failed = HeadObjectError::StorageError(StorageError::Timeout);
    let answer = child_result(Err::<HeadObjectResult, _>(failed));
    let Err(CallError(error)) = check.next(answer) else {
        panic!("an unreadable object vouches for nothing");
    };
    assert_eq!(error.kind, ErrorKind::NotFound);
}

#[test]
fn no_objects_pass() {
    let named = NamedObjects::default();
    let mut check = CheckObjects::new(&scope(), &install(), named);
    assert_eq!(check.first(), Ok(Next::Done(())));
}

fn change(input: ChangeInput) -> PrepareChange {
    let expected = record().last_event_id.to_string();
    let revision = (resource(), expected);
    let revision = (revision.0.as_str(), revision.1.as_str());
    PrepareChange::new(place(), revision, intent(), input).unwrap()
}

fn up_to_written(prepare: &mut PrepareChange) {
    assert!(is_child(&prepare.first()));
    assert!(is_child(&prepare.next(granted_result())));
    assert!(is_child(&prepare.next(summary("{}"))));
    assert!(is_child(&prepare.next(granted_result())));
}

#[test]
fn change_refuses_revision() {
    let refused = PrepareChange::new(
        place(),
        (&resource(), "x"),
        intent(),
        ChangeInput::File(None),
    );
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
}

#[test]
fn change_answers_earlier() {
    let mut prepare = change(ChangeInput::File(None));
    up_to_written(&mut prepare);
    let earlier = child_result(Ok::<bool, StoreError>(true));
    assert!(is_child(&prepare.next(earlier)));
    let settled = Begun::Settled("reply".to_string());
    let answer = prepare.next(child_result(Ok::<_, StoreError>(settled)));
    let Ok(Next::Done(CallStep::Replied(reply))) = answer else {
        panic!("a settled call answers its reply");
    };
    assert_eq!(reply, "reply");
}

#[test]
fn change_proceeds_fresh() {
    let mut prepare = change(ChangeInput::File(None));
    up_to_written(&mut prepare);
    let first_time = child_result(Ok::<bool, StoreError>(false));
    assert!(is_child(&prepare.next(first_time)));
    let Ok(Next::Done(CallStep::Proceed(ready))) = prepare.next(fresh()) else {
        panic!("a fresh call goes on");
    };
    assert_eq!(ready.expected, record().last_event_id);
    assert_eq!(ready.entity, None);
}

#[test]
fn create_checks_arguments() {
    let refused = |path, key| PrepareCreate::new(&scope(), (path, key), "{}", intent()).is_err();
    assert!(refused("", "key"));
    assert!(refused("a@b", "key"));
    assert!(refused("a", ""));
    assert!(!refused("a", "key"));
}

#[test]
fn create_keeps_failure() {
    let mut create = PrepareCreate::new(&scope(), ("a", "key"), "{}", intent()).unwrap();
    create.granted = Some(access());
    create.phase = CreatePhase::Mint;
    let failed = child_result(Err::<PinnedCreate, _>(CreateAuthorizedError::EmptyPath));
    let Ok(Next::Done(CallStep::Proceed(ready))) = create.next(failed) else {
        panic!("a failed mint is recorded by the caller");
    };
    assert!(ready.minted.is_err());
}

#[test]
fn create_answers_earlier() {
    let mut create = PrepareCreate::new(&scope(), ("a", "key"), "{}", intent()).unwrap();
    create.phase = CreatePhase::Begin;
    let settled = child_result(Ok::<_, StoreError>(Begun::Settled("r".to_string())));
    let answer = create.next(settled);
    assert!(matches!(answer, Ok(Next::Done(CallStep::Replied(_)))));
}

fn configured() -> CheckConfigured {
    let mut check = CheckConfigured::secret(&scope(), "slot");
    assert!(is_child(&check.first()));
    assert!(is_child(&check.next(checked_install())));
    check
}

#[test]
fn configured_reads_config() {
    let mut check = configured();
    assert!(is_child(&check.next(granted_result())));
    let config = fixtures::config(&install(), json!({}));
    let Ok(Next::Done(call)) = check.next(config_result(config)) else {
        panic!("the config ends the check");
    };
    assert_eq!(call.install, install());
}

fn connection_extra() -> Value {
    json!({ "connections": { "api": { "url": "https://api.example.org/v1/" } } })
}

fn connection() -> PrepareConnection {
    let ask = ConnectionAsk {
        connection: "api".to_string(),
        method: Method::Get,
        path: "records".to_string(),
        query: Vec::new(),
        headers: Vec::new(),
    };
    PrepareConnection::new(&scope(), ask)
}

#[test]
fn connection_builds_request() {
    let mut prepare = connection();
    assert!(is_child(&prepare.first()));
    assert!(is_child(&prepare.next(checked_install())));
    assert!(is_child(&prepare.next(granted_result())));
    let config = fixtures::config(&install(), connection_extra());
    let Ok(Next::Done(plan)) = prepare.next(config_result(config)) else {
        panic!("a declared connection is planned");
    };
    assert_eq!(
        plan.outgoing.url.as_str(),
        "https://api.example.org/v1/records"
    );
    assert_eq!(plan.slot, None);
}

fn reply_context() -> RequestContext {
    RequestContext {
        connection: "api".to_string(),
        base: "https://api.example.org/v1/".to_string(),
        method: "GET".to_string(),
        path: "records".to_string(),
        query: Vec::new(),
        headers: Vec::new(),
    }
}

#[test]
fn refused_reply_hidden() {
    let mut check = CheckReply::new(&scope(), &reply_context());
    assert!(is_child(&check.first()));
    let denied = child_result(Err::<PluginInstall, _>(refusal()));
    assert_eq!(check.next(denied), Ok(Next::Done(None)));
}

fn native() -> NativeCall {
    let request = WorkRequest::Copy(CopyRequest {
        source: "a".to_string(),
        source_version: None,
        target: "b".to_string(),
        ordinal: 0,
    });
    let minted = ((5, Ulid::from_parts(6, 6)), None);
    let mut call = NativeCall::new(&scope(), intent(), request, minted);
    assert!(is_child(&call.first()));
    call
}

#[test]
fn native_records_start() {
    let mut call = native();
    assert!(is_child(&call.next(fresh())));
    let work = Ulid::from_parts(7, 7);
    assert!(is_child(&call.next(child_result(Ok::<_, CallError>(work)))));
    let kept = child_result(Ok::<_, CallFailure>(work.to_string()));
    assert_eq!(call.next(kept), Ok(Next::Done(work.to_string())));
}

#[test]
fn native_repeats_unknown() {
    let mut call = native();
    let unknown = Begun::Unknown("lost".to_string());
    let answer = child_result(Ok::<_, StoreError>(unknown));
    assert!(is_child(&call.next(answer)));
}

#[test]
fn native_marks_change() {
    let mut call = native();
    let answer = child_result(Ok::<_, StoreError>(Begun::Changed));
    let failure = call.next(answer).unwrap_err();
    assert!(failure.changed);
    assert_eq!(failure.error.kind, ErrorKind::Conflict);
}
