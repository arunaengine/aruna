//! Tests the steps of a run's host calls as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::errors::StorageError;
use serde_json::json;

use super::*;
use crate::jobs::repository::test_support::registry_record;
use crate::metadata::api::list_types::ListedMetadataDocument;
use crate::plugins::check::tests::access_rows;
use crate::plugins::fixtures::{self, config_result, is_child, node};
use crate::plugins::rate::tests::rate_answer;
use crate::plugins::rows::tests::child_result;
use crate::plugins::submit::CopyRequest;
use crate::tests::calls::compiled;
use aruna_test_support::plugins::queued;

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
    // The checked call is counted against the install's rate before it may run.
    let counted = admit.next(checked_install());
    assert!(matches!(counted, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(admit.next(rate_answer(true)), Ok(Next::Done(install())));
    let mut fast = AdmitCall::new(&scope(), None);
    fast.first().unwrap();
    fast.next(checked_install()).unwrap();
    let limited = fast.next(rate_answer(false));
    assert!(matches!(limited, Err(CallError(error)) if error.kind == ErrorKind::Limit));
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
    call.next(checked_install()).unwrap();
    let Ok(Next::Done(access)) = call.next(rate_answer(true)) else {
        panic!("a covered target is granted");
    };
    assert!(access.auth.path_restrictions.is_some());
    let other = "/elsewhere";
    let mut call = GrantedCall::target(&scope(), Permission::MetadataRead, other);
    call.first().unwrap();
    call.next(checked_install()).unwrap();
    assert!(call.next(rate_answer(true)).is_err());
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
    let denied = child_result(Err::<(), _>(refusal()));
    assert_eq!(read.next(denied).unwrap_err(), refusal());
}

#[test]
fn read_reuses_admission() {
    // The exported path is matched against the install the listing admitted, so the call is
    // checked and rated once: a covered path needs only its authorization.
    let jsonld = json!({"@graph": [{"@id": "ro-crate-metadata.json"}, {"@id": "./"}]});
    let view = RoCrateExportView::Summary;
    let mut read = ReadDataset::new(place(), &resource(), view);
    read.first().unwrap();
    read.next(granted_result()).unwrap();
    read.next(summary(&jsonld.to_string())).unwrap();
    let Ok(Next::Done(document)) = read.next(child_result(Ok::<_, CallError>(()))) else {
        panic!("an authorized path reads the dataset");
    };
    assert_eq!(document.revision, record().last_event_id.to_string());
    // A path outside the grants is refused at once, without another call.
    let mut outside = ReadDataset::new(place(), &resource(), view);
    outside.first().unwrap();
    outside.next(granted_result()).unwrap();
    let mut elsewhere = record();
    elsewhere.permission_path = "/elsewhere".to_string();
    let export = child_result(Ok::<_, MetadataApiError>(ExportMetadataResult::Summary {
        record: elsewhere,
        jsonld: jsonld.to_string(),
    }));
    let Err(CallError(error)) = outside.next(export) else {
        panic!("an uncovered path is refused");
    };
    assert_eq!(error.kind, ErrorKind::NotGranted);
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

// The documents of a page share one read of the rules and the policies and one compile.
#[test]
fn list_keeps_authorized() {
    let covered = |name: &str| format!("{}/{name}", meta_root());
    let mut list = ListDatasets::new(&scope(), None);
    assert!(is_child(&list.first()));
    assert!(is_child(&list.next(granted_result())));
    let answer = listed(&["/outside".to_string(), covered("a"), covered("b")]);
    let [rules, policies] = access_rows(&install(), &format!("{}/**", meta_root()));
    assert!(matches!(list.next(answer), Ok(Next::Call(IoCall::Read(_)))));
    assert!(matches!(list.next(rules), Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(compile)) = list.next(policies) else {
        panic!("the read policies compile");
    };
    let Ok(Next::Done(listing)) = list.next(compiled(compile)) else {
        panic!("the second document needs no more reads");
    };
    assert_eq!(listing.resources.len(), 2);
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
    let Err(CallError(error)) = list.next(IoResult::Done) else {
        panic!("a failed check fails the page");
    };
    assert_eq!(error.kind, ErrorKind::Unavailable);
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
fn native_starts_task() {
    let request = TaskRequest {
        task: "map".to_string(),
        parameters: Default::default(),
        inputs: Vec::new(),
        files: Vec::new(),
        outputs: Vec::new(),
        ordinal: 0,
    };
    let node = crate::tests::records::local_ingress();
    let mut call = NativeCall::task(&scope(), intent(), request, node);
    assert!(is_child(&call.first()));
    assert!(is_child(&call.next(fresh())));
    assert_eq!((call.work, call.phase), (None, NativePhase::Start));
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

fn ready_change() -> ReadyChange {
    ReadyChange {
        record: Box::new(record()),
        granted: access(),
        document_id: Ulid::from_parts(4, 4),
        expected: record().last_event_id,
        entity: None,
    }
}

fn apply() -> ApplyChange {
    let mutation = UpdateDocumentMutation::RemoveDataEntity {
        id: "file".to_string(),
    };
    let scope = scope();
    let mut apply = ApplyChange::new((&scope, None), ready_change(), mutation, intent());
    assert!(is_child(&apply.first()));
    apply
}

#[test]
fn apply_records_revision() {
    let mut change = apply();
    let routed = child_result(Ok::<_, MetadataWriteError>(record()));
    assert!(is_child(&change.next(routed)));
    let kept = child_result(Ok::<_, CallFailure>("reply".to_string()));
    assert_eq!(change.next(kept), Ok(Next::Done("reply".to_string())));
}

#[test]
fn apply_records_failure() {
    let mut change = apply();
    let refused = child_result(Err::<MetadataRegistryRecord, _>(
        MetadataWriteError::Forbidden,
    ));
    assert!(is_child(&change.next(refused)));
    let failed = CallFailure::from(refusal());
    let kept = child_result(Err::<String, _>(failed.clone()));
    assert_eq!(change.next(kept), Err(failed));
}

fn plan(mutation: ChangeMutation) -> ChangePlan {
    ChangePlan {
        scope: scope(),
        network_node: None,
        mutation,
        intent: intent(),
    }
}

fn checked(mutation: ChangeMutation) -> Checked<PrepareChange, ChangePlan> {
    let mut write = Checked::new(change(ChangeInput::File(None)), plan(mutation));
    assert!(is_child(&write.first()), "the checks run first");
    write
}

/// A failure that marks the run as changed.
fn marked_failure() -> CallFailure {
    CallFailure {
        changed: true,
        ..CallFailure::from(refusal())
    }
}

#[test]
fn checked_proceeds_once() {
    let mut write = checked(ChangeMutation::Dataset);
    let ready = Ok::<_, CallFailure>(CallStep::Proceed(ready_change()));
    assert!(is_child(&write.next(child_result(ready))), "then the write");
    let failed = write.next(child_result(Err::<String, _>(marked_failure())));
    let unmarked = Err(CallFailure::from(refusal()));
    assert_eq!(failed, unmarked, "a late failure is no change");
}

#[test]
fn checked_answers_earlier() {
    let given = ChangeMutation::Given(UpdateDocumentMutation::RemoveDataEntity {
        id: "file".to_string(),
    });
    let mut write = checked(given);
    let replied = CallStep::<ReadyChange>::Replied("reply".into());
    let answer = write.next(child_result(Ok::<_, CallFailure>(replied)));
    assert_eq!(answer, Ok(Next::Done("reply".into())));
    let mut refused = checked(ChangeMutation::Dataset);
    let checks = child_result(Err::<CallStep<ReadyChange>, _>(marked_failure()));
    let kept = Err(marked_failure());
    assert_eq!(refused.next(checks), kept, "checks keep their mark");
}

#[test]
fn dataset_upserts_entity() {
    let entity = "{\"@id\":\"./\"}";
    let ready = || ReadyChange {
        entity: Some(entity.into()),
        ..ready_change()
    };
    let upsert = UpdateDocumentMutation::UpsertContextualEntity {
        jsonld: entity.into(),
    };
    let scope = scope();
    let direct = ApplyChange::new((&scope, None), ready(), upsert, intent());
    assert_eq!(plan(ChangeMutation::Dataset).call(ready()), direct);
}
