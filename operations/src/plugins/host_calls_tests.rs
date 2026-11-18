//! Tests the decisions behind host calls as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::rows::tests::child_result;
use crate::plugins::work::tests::sample_install;

fn intent(ordinal: u32) -> CallIntent {
    CallIntent {
        ordinal,
        digest: [ordinal as u8; 32],
    }
}

fn run() -> Ulid {
    Ulid::from_parts(4, 4)
}

fn written() -> IoResult {
    child_result(Ok::<(), StoreError>(()))
}

fn is_child<T>(next: Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

fn kind_of<T: std::fmt::Debug>(result: Result<T, CallFailure>) -> ErrorKind {
    result.unwrap_err().error.kind
}

fn data_path(install: &PluginInstall, rest: &str) -> String {
    let realm = install.account.realm_id;
    format!(
        "/{realm}/g/{}/data/{}/{rest}",
        install.group_id, install.receiver
    )
}

fn place(install: &PluginInstall) -> (RealmId, NodeId) {
    (install.account.realm_id, install.receiver)
}

#[test]
fn begun_answers() {
    assert_eq!(begun_reply(Begun::Fresh), Ok(None));
    let settled = begun_reply(Begun::Settled("r".into()));
    assert_eq!(settled, Ok(Some("r".to_string())));
    let changed = begun_reply(Begun::Changed).unwrap_err();
    assert!(changed.changed);
    assert_eq!(changed.error.kind, ErrorKind::Conflict);
    let unknown = begun_reply(Begun::Unknown("why".into())).unwrap_err();
    assert_eq!(
        (unknown.error.kind, unknown.changed),
        (ErrorKind::UnknownOutcome, false)
    );
}

#[test]
fn reply_settles_intent() {
    let mut record = RecordCall::new(run(), intent(1), Ok("r".into()));
    assert!(is_child(record.first().unwrap()));
    assert_eq!(record.next(written()), Ok(Next::Done("r".to_string())));
}

#[test]
fn lost_settlement_unknown() {
    let mut record = RecordCall::new(run(), intent(1), Ok("r".into()));
    record.first().unwrap();
    let failed = child_result(Err::<(), _>(StoreError("down".into())));
    assert!(is_child(record.next(failed).unwrap()));
    assert_eq!(kind_of(record.next(written())), ErrorKind::UnknownOutcome);
}

#[test]
fn refusal_clears_intent() {
    let refused = HostError::new(ErrorKind::Denied, "no");
    let end = Err(CallEnd::None(refused.clone()));
    let mut record = RecordCall::new(run(), intent(1), end);
    assert!(is_child(record.first().unwrap()));
    assert_eq!(record.next(written()), Err(refused.into()));
    let mut failing = RecordCall::new(
        run(),
        intent(1),
        Err(CallEnd::None(HostError::new(ErrorKind::Denied, "no"))),
    );
    failing.first().unwrap();
    let failed = child_result(Err::<(), _>(StoreError("down".into())));
    assert_eq!(kind_of(failing.next(failed)), ErrorKind::Unavailable);
}

#[test]
fn maybe_marks_unknown() {
    let end = Err(CallEnd::Maybe("lost".into()));
    let mut record = RecordCall::new(run(), intent(1), end);
    assert!(is_child(record.first().unwrap()));
    let failure = record.next(written()).unwrap_err();
    assert_eq!(failure.error.message, "lost");
}

fn put(ordinal: u32) -> StateCall {
    let change = StateChange::Put(StateWrite {
        key: "k".into(),
        value: vec![1],
        expected: None,
        intent: intent(ordinal),
    });
    StateCall::new((run(), Ulid::from_parts(1, 1)), change)
}

fn remove() -> StateCall {
    let change = StateChange::Remove(StateRemoval {
        key: "k".into(),
        expected: 1,
        intent: intent(1),
    });
    StateCall::new((run(), Ulid::from_parts(1, 1)), change)
}

fn begun(begun: Begun) -> IoResult {
    child_result(Ok::<_, StoreError>(begun))
}

#[test]
fn fresh_put_writes() {
    let mut call = put(1);
    assert!(is_child(call.first().unwrap()));
    assert!(is_child(call.next(begun(Begun::Fresh)).unwrap()));
    let version = child_result(Ok::<u64, PutError>(7));
    assert_eq!(call.next(version), Ok(Next::Done(7)));
}

#[test]
fn settled_put_answers() {
    let mut call = put(1);
    let answer = call.next(begun(Begun::Settled("9".into())));
    assert_eq!(answer, Ok(Next::Done(9)));
    let mut bad = put(1);
    let answer = bad.next(begun(Begun::Settled("x".into())));
    assert_eq!(kind_of(answer), ErrorKind::Unavailable);
    let mut settled = remove();
    let answer = settled.next(begun(Begun::Settled(String::new())));
    assert_eq!(answer, Ok(Next::Done(0)));
}

#[test]
fn changed_put_flagged() {
    let mut call = put(1);
    assert!(call.next(begun(Begun::Changed)).unwrap_err().changed);
}

#[test]
fn refused_put_recorded() {
    let mut call = put(1);
    call.next(begun(Begun::Fresh)).unwrap();
    let refusal = PutError::Refused("full".into(), StateRefusal::Limit);
    let answer = call.next(child_result(Err::<u64, _>(refusal)));
    assert!(is_child(answer.unwrap()));
    let failure = CallFailure::from(HostError::new(ErrorKind::Limit, "full"));
    let recorded = child_result(Err::<String, _>(failure.clone()));
    assert_eq!(call.next(recorded), Err(failure));
}

#[test]
fn removal_answers_zero() {
    let mut call = remove();
    call.next(begun(Begun::Fresh)).unwrap();
    let removed = child_result(Ok::<(), PutError>(()));
    assert_eq!(call.next(removed), Ok(Next::Done(0)));
}

#[test]
fn info_reports_mark() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let path = data_path(&install, "b/k");
    let version = Ulid::from_parts(9, 9).to_string();
    let mut info = ObjectInfoCall::new(place(&install), &install, &path, None).unwrap();
    assert!(is_child(info.first().unwrap()));
    let located = ("b".to_string(), "k".to_string());
    let answer = info.next(child_result(Ok::<_, CallError>(located)));
    assert!(is_child(answer.unwrap()));
    let head = HeadFacts {
        size: 3,
        version: version.clone(),
    };
    assert!(is_child(
        info.next(child_result(Ok::<_, ()>(head))).unwrap()
    ));
    let Next::Done(found) = info
        .next(child_result(Ok::<bool, StoreError>(true)))
        .unwrap()
    else {
        panic!("the mark ends the call");
    };
    assert_eq!((found.path.as_str(), found.size), ("b/k", 3));
    assert!(found.marked);
    assert_eq!(found.version, version);
}

#[test]
fn info_missing_object() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let path = data_path(&install, "b/k");
    let mut info = ObjectInfoCall::new(place(&install), &install, &path, None).unwrap();
    let located = ("b".to_string(), "k".to_string());
    info.next(child_result(Ok::<_, CallError>(located)))
        .unwrap();
    let gone = child_result(Err::<HeadFacts, ()>(()));
    assert_eq!(kind_of(info.next(gone)), ErrorKind::NotFound);
    let refused = ObjectInfoCall::new(place(&install), &install, &path, Some("x"));
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
}

#[test]
fn listing_refuses_cursor() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let path = data_path(&install, "b");
    let bad = ObjectListCall::new(place(&install), &install, &path, Some("zz"));
    assert_eq!(bad.unwrap_err().kind, ErrorKind::Invalid);
}

#[test]
fn listing_filters_objects() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let path = data_path(&install, "b");
    let mut list = ObjectListCall::new(place(&install), &install, &path, None).unwrap();
    assert!(is_child(list.first().unwrap()));
    let located = ("b".to_string(), String::new());
    let located = list.next(child_result(Ok::<_, CallError>(located)));
    assert!(is_child(located.unwrap()));
    let listed = Listed {
        objects: Vec::new(),
        cursor: Some("c".into()),
    };
    let filter = list.next(child_result(Ok::<_, String>(listed)));
    assert!(is_child(filter.unwrap()));
    let shown = child_result(Ok::<Vec<ObjectInfo>, CallFailure>(Vec::new()));
    let expected = ObjectPage {
        objects: Vec::new(),
        cursor: Some("c".into()),
    };
    assert_eq!(list.next(shown), Ok(Next::Done(expected)));
}

fn listed_object() -> ListedObject {
    ("k".to_string(), 1, "v".to_string())
}

#[test]
fn ungranted_objects_hidden() {
    let mut install = sample_install(Ulid::from_parts(1, 1));
    install.grants.clear();
    let listed = vec![listed_object()];
    let mut visible = VisibleObjects::new(place(&install), &install, "b", listed);
    assert_eq!(visible.first(), Ok(Next::Done(Vec::new())));
}

fn authorizing() -> VisibleObjects {
    let install = sample_install(Ulid::from_parts(1, 1));
    let mut visible = VisibleObjects::new(place(&install), &install, "b", Vec::new());
    visible.phase = Some(VisiblePhase::Authorizing(listed_object()));
    visible
}

#[test]
fn denied_objects_hidden() {
    let denied = CallError(HostError::new(ErrorKind::Denied, "no"));
    let mut visible = authorizing();
    let answer = visible.next(child_result(Err::<(), _>(denied)));
    assert_eq!(answer, Ok(Next::Done(Vec::new())));
    let down = CallError(unavailable("later"));
    let answer = authorizing().next(child_result(Err::<(), _>(down)));
    assert_eq!(kind_of(answer), ErrorKind::Unavailable);
}

#[test]
fn allowed_object_shown() {
    let mut visible = authorizing();
    let Next::Done(shown) = visible.next(child_result(Ok::<(), CallError>(()))).unwrap() else {
        panic!("a version that is no id has no mark to read");
    };
    assert_eq!((shown[0].path.as_str(), shown[0].marked), ("b/k", false));
}
