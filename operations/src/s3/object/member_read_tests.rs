//! Tests the routing of member reads and the request one read sends, call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::distribution::DistributionRevision;

use super::*;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn range() -> Ulid {
    Ulid::from_bytes([5; 16])
}

fn set() -> Ulid {
    Ulid::from_bytes([3; 16])
}

fn caller() -> DelegatedCaller {
    DelegatedCaller {
        user_id: UserId::nil(realm()),
        group_id: Ulid::from_bytes([2; 16]),
        credential_id: "key".to_string(),
        restrictions: None,
        credential_expiry: 0,
    }
}

fn read(version: Option<Ulid>) -> MemberReadOperation {
    let binding = DistributionBinding {
        set_id: set(),
        member_id: Ulid::from_bytes([4; 16]),
        incarnation: 0,
        revision: DistributionRevision::default(),
    };
    let ids = Some((node(1), realm()));
    let object = MemberKey::get("spire", "a b", version);
    MemberReadOperation::new(ids, caller(), binding, object)
}

fn child<T, E>(answer: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(answer))
}

fn body() -> MemberBody {
    let chunks = futures_util::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
    MemberBody {
        status: 200,
        headers: Vec::new(),
        stream: ByteStream::new(chunks),
    }
}

fn refusal(status: u16, code: Option<&str>) -> MemberRefusal {
    MemberRefusal::Refused {
        node_id: node(2),
        status,
        code: code.map(str::to_string),
        message: String::new(),
    }
}

fn rest_read(stream: bool) -> MemberReadOperation {
    let ask = RestAsk {
        local: node(1),
        path: "/x".to_string(),
        bearer: None,
        headers: Vec::new(),
        stream,
    };
    let binding = DistributionBinding {
        set_id: set(),
        member_id: Ulid::from_bytes([4; 16]),
        incarnation: 0,
        revision: DistributionRevision::default(),
    };
    MemberReadOperation::rest(binding, "a", ask)
}

fn rest_routed(read: &mut MemberReadOperation, holders: Vec<NodeId>) {
    read.first().unwrap();
    let route = ReadRoute::Holder {
        range_id: range(),
        node_id: node(2),
    };
    read.next(child::<_, GuardError>(Ok(route))).unwrap();
    read.next(child::<_, IoError>(Ok(holders))).unwrap();
}

fn rest_answer(answer: RestAnswer) -> IoResult {
    child::<_, IoError>(Ok(answer))
}

fn refused(status: u16, code: Option<&str>) -> IoResult {
    child::<MemberBody, _>(Err(refusal(status, code)))
}

fn answered() -> IoResult {
    child::<_, MemberRefusal>(Ok(body()))
}

fn at_coordinator(coordinator: Option<(Ulid, NodeId)>) -> IoResult {
    child::<_, GuardError>(Ok(coordinator))
}

fn coordinated(read: &mut MemberReadOperation) {
    read.first().unwrap();
    read.next(at_coordinator(Some((range(), node(2))))).unwrap();
}

fn pinned_at(read: &mut MemberReadOperation, holder: NodeId) {
    read.first().unwrap();
    let route = ReadRoute::Holder {
        range_id: range(),
        node_id: node(2),
    };
    read.next(child::<_, GuardError>(Ok(route))).unwrap();
    read.next(child::<_, IoError>(Ok(vec![holder]))).unwrap();
    let candidate = HolderCandidate {
        node_id: holder,
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        free_slots: 1,
        bandwidth: 1,
    };
    read.next(child::<_, HolderChoiceError>(Ok(vec![candidate])))
        .unwrap();
}

#[test]
fn own_range_local() {
    let mut read = read(None);
    read.first().unwrap();
    let answer = read.next(at_coordinator(None));
    assert_eq!(answer, Ok(Next::Done(MemberRead::Local(None))));
}

#[test]
fn refusal_ends_read() {
    let mut read = read(None);
    coordinated(&mut read);
    let answer = read.next(refused(403, Some("AccessDenied")));
    assert_eq!(answer, Err(refusal(403, Some("AccessDenied")).into()));
}

#[test]
fn copy_answers_stale() {
    let mut read = read(None);
    coordinated(&mut read);
    let readers = read.next(refused(503, None)).unwrap();
    assert!(matches!(readers, Next::Call(IoCall::Child(_))));
    let copies = (range(), false, vec![node(3)]);
    read.next(child::<_, GuardError>(Ok(copies))).unwrap();
    let Ok(Next::Done(MemberRead::Remote { stale, .. })) = read.next(answered()) else {
        panic!("the copy answers");
    };
    assert_eq!(stale, Some(range()));
}

#[test]
fn copy_refusal_marked() {
    let mut read = read(None);
    coordinated(&mut read);
    read.next(refused(503, None)).unwrap();
    let copies = (range(), false, vec![node(3)]);
    read.next(child::<_, GuardError>(Ok(copies))).unwrap();
    let failure = read.next(refused(404, Some("NoSuchKey"))).unwrap_err();
    assert_eq!(failure.stale, Some(range()));
}

#[test]
fn own_copy_serves() {
    let mut read = read(None);
    coordinated(&mut read);
    read.next(refused(503, None)).unwrap();
    let readers = (range(), true, Vec::<NodeId>::new());
    let answer = read.next(child::<_, GuardError>(Ok(readers)));
    assert_eq!(answer, Ok(Next::Done(MemberRead::Local(Some(range())))));
}

#[test]
fn silent_copies_fail() {
    let mut read = read(None);
    coordinated(&mut read);
    read.next(refused(503, Some("SlowDown"))).unwrap();
    let readers = (range(), false, vec![node(3)]);
    read.next(child::<_, GuardError>(Ok(readers))).unwrap();
    let answer = read.next(refused(503, None));
    assert_eq!(answer, Err(refusal(503, Some("SlowDown")).into()));
}

#[test]
fn replica_miss_retried() {
    let mut read = read(Some(Ulid::from_bytes([9; 16])));
    pinned_at(&mut read, node(3));
    assert_eq!(read.holder, Some(node(3)));
    read.next(refused(404, Some("NoSuchKey"))).unwrap();
    assert_eq!(read.step, ReadStep::Coordinating);
    let answer = read.next(refused(404, Some("NoSuchKey")));
    assert_eq!(answer, Err(refusal(404, Some("NoSuchKey")).into()));
}

#[test]
fn coordinator_miss_final() {
    let mut read = read(Some(Ulid::from_bytes([9; 16])));
    pinned_at(&mut read, node(2));
    let answer = read.next(refused(404, Some("NoSuchKey")));
    assert_eq!(answer, Err(refusal(404, Some("NoSuchKey")).into()));
}

#[test]
fn pinned_read_stays() {
    let mut read = read(Some(Ulid::from_bytes([9; 16])));
    pinned_at(&mut read, node(2));
    assert_eq!(
        read.next(refused(503, None)),
        Err(refusal(503, None).into())
    );
}

#[test]
fn delegated_current_refused() {
    let mut read = read(None).delegated(false);
    read.first().unwrap();
    let failure = read
        .next(at_coordinator(Some((range(), node(2)))))
        .unwrap_err();
    let expected = GuardError::NotCoordinator {
        set_id: set(),
        range_id: range(),
        node_id: node(2),
    };
    assert_eq!(failure.refusal, MemberRefusal::Route(expected));
}

#[test]
fn delegated_copy_local() {
    let answer = read(None).delegated(true).first();
    assert_eq!(answer, Ok(Next::Done(MemberRead::Local(None))));
    let pinned = read(Some(Ulid::from_bytes([9; 16])))
        .delegated(false)
        .first();
    assert_eq!(pinned, Ok(Next::Done(MemberRead::Local(None))));
}

#[test]
fn miss_reaches_coordinator() {
    let mut read = read(None).after_miss(false);
    coordinated(&mut read);
    assert_eq!(read.step, ReadStep::Missed);
    let answer = read.next(answered());
    assert!(matches!(
        answer,
        Ok(Next::Done(MemberRead::Remote { stale: None, .. }))
    ));
}

#[test]
fn silent_keeps_miss() {
    let mut read = read(None).after_miss(false);
    coordinated(&mut read);
    let answer = read.next(refused(503, None));
    assert_eq!(answer, Ok(Next::Done(MemberRead::Local(Some(range())))));
}

#[test]
fn delegated_miss_stays() {
    let mut read = read(None).after_miss(true);
    read.first().unwrap();
    let answer = read.next(at_coordinator(Some((range(), node(2)))));
    assert_eq!(answer, Ok(Next::Done(MemberRead::Local(None))));
}

fn fetch(object: MemberKey, copy_read: bool) -> MemberCallOperation {
    let target = MemberTarget {
        set_id: set(),
        range_id: range(),
        node_id: node(2),
    };
    let mut request = object.request();
    if copy_read {
        (request.headers).push((COPY_READ_HEADER.to_string(), "1".to_string()));
    }
    let mut call = MemberCallOperation::read(Some((node(1), realm())), caller(), target, request);
    call.stamp(10_000);
    call
}

fn urls(s3: Option<&str>) -> IoResult {
    let urls = NodeUrls {
        api: None,
        s3: s3.map(str::to_string),
    };
    child::<_, NodeInfoError>(Ok(Some(urls)))
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn member_answer(answer: MemberAnswer) -> IoResult {
    event(Event::Outbound(OutboundEvent::MemberAnswered(Box::new(
        answer,
    ))))
}

#[test]
fn fetch_binds_request() {
    let version = Ulid::from_bytes([9; 16]);
    let mut get = fetch(MemberKey::get("spire", "a b", Some(version)), true);
    get.first().unwrap();
    let Ok(Next::Call(IoCall::Effect(effect))) = get.next(urls(Some("http://s3/"))) else {
        panic!("the delegation is signed");
    };
    let Effect::Net(NetEffect::DelegationSign(delegation)) = *effect else {
        panic!("the delegation is signed");
    };
    assert_eq!(delegation.operation, "GetObject");
    assert_eq!(delegation.target, node(2));
    assert_eq!(delegation.credential_id, "key");
    // A caller without a credential is bounded by a short window.
    assert_eq!(delegation.credential_expiry, 10 + ANONYMOUS_SECS);
    let query = format!("versionId={version}");
    let bound = [("accept", ACCEPT_ANY), (COPY_READ_HEADER, "1")];
    let digest = request_digest("GET", "spire", "a b", Some(&query), &bound);
    assert_eq!(delegation.request_digest, digest);
    let signed = SignedDelegation {
        signature: iroh::SecretKey::from_bytes(&[1; 32]).sign(b"delegation"),
        delegation: *delegation,
    };
    let header = signed.encode().unwrap();
    let signed = Event::Net(NetEvent::DelegationSign(Some(Box::new(signed))));
    let Ok(Next::Call(IoCall::Effect(effect))) = get.next(event(signed)) else {
        panic!("the read is sent");
    };
    let Effect::Outbound(OutboundEffect::MemberFetch(request)) = *effect else {
        panic!("the read is sent");
    };
    assert_eq!(request.method, MemberMethod::Get);
    assert_eq!(request.answer_limit, Some(ANSWER_LIMIT));
    assert_eq!(request.url, format!("http://s3/spire/a%20b?{query}"));
    let expected = [
        ("accept", ACCEPT_ANY),
        (COPY_READ_HEADER, "1"),
        (DELEGATION_HEADER, header.as_str()),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()));
    assert_eq!(request.headers, expected);
}

#[test]
fn head_keeps_headers() {
    let mut object = MemberKey::head("spire", "a");
    object.headers = vec![
        ("range".to_string(), "bytes=0-1".to_string()),
        ("range".to_string(), "bytes=0-2".to_string()),
    ];
    let mut head = fetch(object, false);
    head.first().unwrap();
    let Ok(Next::Call(IoCall::Effect(effect))) = head.next(urls(Some("http://s3"))) else {
        panic!("the delegation is signed");
    };
    let Effect::Net(NetEffect::DelegationSign(delegation)) = *effect else {
        panic!("the delegation is signed");
    };
    assert_eq!(delegation.operation, "HeadObject");
    // A later value of a header replaces an earlier one, as on the wire.
    let bound = [("accept", ACCEPT_ANY), ("range", "bytes=0-2")];
    let digest = request_digest("HEAD", "spire", "a", None, &bound);
    assert_eq!(delegation.request_digest, digest);
}

#[test]
fn silent_member_avoided() {
    let mut get = fetch(MemberKey::get("spire", "a", None), false);
    get.step = CallStep::Answer;
    let avoid = NetEffect::AvoidHolder {
        node_id: node(2),
        now_ms: 10_000,
    };
    let expected = IoCall::from(Effect::Net(avoid));
    let answer = get.next(member_answer(MemberAnswer::Silent));
    assert_eq!(answer, Ok(Next::Call(expected)));
    let answer = get.next(event(Event::Net(NetEvent::HolderAvoided)));
    assert_eq!(answer, Err(get.unreached(false)));
}

#[test]
fn late_member_named() {
    let mut get = fetch(MemberKey::get("spire", "a", None), false);
    get.step = CallStep::Answer;
    get.next(member_answer(MemberAnswer::Late)).unwrap();
    let answer = get.next(event(Event::Net(NetEvent::HolderAvoided)));
    assert_eq!(answer, Err(get.unreached(true)));
}

#[test]
fn refusal_keeps_holder() {
    let mut get = fetch(MemberKey::get("spire", "a", None), false);
    get.step = CallStep::Answer;
    let answer = get.next(member_answer(MemberAnswer::Refused {
        status: 403,
        body: "<Error><Code>AccessDenied</Code></Error>".to_string(),
    }));
    assert_eq!(answer, Err(refusal(403, Some("AccessDenied"))));
}

#[test]
fn unpublished_member_avoided() {
    let mut get = fetch(MemberKey::get("spire", "a", None), false);
    get.first().unwrap();
    assert!(matches!(
        get.next(urls(None)),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(get.step, CallStep::Avoid(_)));
}

#[test]
fn write_signs_body() {
    let target = MemberTarget {
        set_id: set(),
        range_id: range(),
        node_id: node(2),
    };
    let chunks = futures_util::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
    let request = MemberRequest {
        method: MemberMethod::Put,
        operation: "PutObject",
        bucket: "spire".to_string(),
        key: "a".to_string(),
        query: None,
        headers: vec![("content-length".to_string(), "5".to_string())],
        body: MemberPayload::Stream(ByteStream::new(chunks), None),
    };
    let ids = Some((node(1), realm()));
    let mut put = MemberCallOperation::write(ids, caller(), target, request);
    put.first().unwrap();
    let Ok(Next::Call(IoCall::Effect(effect))) = put.next(urls(Some("http://s3"))) else {
        panic!("the delegation is signed");
    };
    let Effect::Net(NetEffect::DelegationSign(delegation)) = *effect else {
        panic!("the delegation is signed");
    };
    assert!(matches!(delegation.payload, Payload::Framed { .. }));
    // The framed body is longer by its trailer, which the delegation binds.
    let length = (5 + TRAILER_BYTES).to_string();
    let bound = [("accept", ACCEPT_ANY), ("content-length", length.as_str())];
    let digest = request_digest("PUT", "spire", "a", None, &bound);
    assert_eq!(delegation.request_digest, digest);
}

#[test]
fn write_failure_kept() {
    let target = MemberTarget {
        set_id: set(),
        range_id: range(),
        node_id: node(2),
    };
    let request = MemberKey::get("spire", "a", None).request();
    let ids = Some((node(1), realm()));
    let mut put = MemberCallOperation::write(ids, caller(), target, request);
    put.step = CallStep::Answer;
    let answer = put.next(member_answer(MemberAnswer::Silent));
    assert_eq!(answer, Err(put.unreached(false)));
}

#[test]
fn text_reads_body() {
    let mut text = MemberTextOperation::of_body(body());
    assert!(matches!(text.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let collected = JobEvent::CollectedUpTo(Ok(Some(b"page".to_vec())));
    let answer = text.next(event(Event::Job(collected)));
    assert_eq!(answer, Ok(Next::Done("page".to_string())));
}

#[test]
fn oversized_text_broken() {
    let mut text = MemberTextOperation::of_body(body());
    text.first().unwrap();
    let answer = text.next(event(Event::Job(JobEvent::CollectedUpTo(Ok(None)))));
    assert!(matches!(answer, Err(MemberRefusal::Broken(_))));
}

#[test]
fn rest_copies_precede() {
    let mut read = rest_read(false);
    rest_routed(&mut read, vec![node(1), node(3), node(2)]);
    assert_eq!(read.holder, Some(node(3)));
    let refused = RestAnswer::Refused {
        status: 404,
        body: r#"{"error":"missing"}"#.to_string(),
    };
    read.next(rest_answer(refused)).unwrap();
    assert_eq!(read.holder, Some(node(2)));
    let answer = read.next(rest_answer(RestAnswer::Body(body())));
    assert!(matches!(answer, Ok(Next::Done(MemberRead::Remote { .. }))));
}

#[test]
fn rest_coordinator_decides() {
    let mut read = rest_read(false);
    rest_routed(&mut read, Vec::new());
    let refused = RestAnswer::Refused {
        status: 404,
        body: r#"{"error":"missing"}"#.to_string(),
    };
    let failure = read.next(rest_answer(refused)).unwrap_err();
    assert_eq!(failure.refusal.status(), 404);
}

#[test]
fn rest_silence_exhausts() {
    let mut read = rest_read(true);
    rest_routed(&mut read, Vec::new());
    let failure = read.next(rest_answer(RestAnswer::Silent)).unwrap_err();
    let expected = MemberRefusal::NoHolder {
        set_id: set(),
        range_id: range(),
    };
    assert_eq!(failure.refusal, expected);
}

#[test]
fn rest_stream_continues() {
    let mut read = rest_read(true);
    rest_routed(&mut read, vec![node(3)]);
    let mut miss = body();
    miss.status = 404;
    read.next(rest_answer(RestAnswer::Body(miss))).unwrap();
    assert_eq!(read.holder, Some(node(2)));
}

#[test]
fn rest_miss_forwards() {
    let mut read = rest_read(false).after_miss(false);
    read.first().unwrap();
    read.next(at_coordinator(Some((range(), node(2))))).unwrap();
    assert_eq!(read.holder, Some(node(2)));
    assert_eq!(read.step, ReadStep::RestAnswer);
}
