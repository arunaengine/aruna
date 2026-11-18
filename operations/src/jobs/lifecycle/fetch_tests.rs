//! Call and result tests of the family fetch from its holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, MetadataRequestEffect, NetEffect};
use aruna_core::events::{
    Event, JobRecordPage, JobRecordRejection, MetadataAnswer, NetEvent, PeerRequestError,
};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobFamilyRecord;

use super::FetchFamilyOperation;
use crate::jobs::lifecycle::LifecycleError;
use crate::metadata::protocol::{JobPageReply, MetadataTransportMessage, encode_message};
use crate::tests::records::{Family, node};

fn fetch(family: &Family) -> FetchFamilyOperation {
    FetchFamilyOperation::new(
        family.placement,
        vec![node(2), node(3)],
        (family.submission_id, family.request_digest),
        Duration::from_secs(10),
    )
}

fn frame(family: &Family) -> Vec<u8> {
    encode_message(&MetadataTransportMessage::ForwardRecordPage {
        placement: family.placement,
        submission_id: family.submission_id,
        request_digest: Some(family.request_digest),
        cursor: None,
        limit: Default::default(),
    })
    .expect("frame encodes")
}

fn ask(
    family: &Family,
    peer: NodeId,
    timeout: Duration,
) -> Next<(Option<NodeId>, Vec<JobRecordFrame>)> {
    Next::Call(IoCall::Effect(Box::new(Effect::Net(
        NetEffect::MetadataRequest(Box::new(MetadataRequestEffect {
            peer,
            frame: frame(family),
            timeout,
        })),
    ))))
}

fn answer(
    peer: NodeId,
    elapsed: u64,
    result: Result<MetadataTransportMessage, PeerRequestError>,
) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        MetadataAnswer {
            peer,
            elapsed: Duration::from_secs(elapsed),
            result: result.map(|message| encode_message(&message).expect("reply encodes")),
        },
    )))))
}

fn spec_record(family: &Family) -> JobRecordFrame {
    let envelope = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    JobRecordFrame::new(envelope).expect("bounded record")
}

#[test]
fn takes_first_page() {
    let family = Family::new([5u8; 32]);
    let mut fetch = fetch(&family);
    assert_eq!(
        fetch.first(),
        Ok(ask(&family, node(2), Duration::from_secs(10)))
    );
    // A refusing holder is passed over; the next one gets the time that is left.
    let refused = MetadataTransportMessage::ForwardedRecordPage {
        result: Err(JobRecordRejection::NotHolder),
    };
    assert_eq!(
        fetch.next(answer(node(2), 3, Ok(refused))),
        Ok(ask(&family, node(3), Duration::from_secs(7)))
    );
    let record = spec_record(&family);
    let page = JobRecordPage::new(vec![record.clone()]).expect("bounded page");
    let reply = MetadataTransportMessage::ForwardedRecordPage {
        result: Ok(JobPageReply { page, next: None }),
    };
    assert_eq!(
        fetch.next(answer(node(3), 1, Ok(reply))),
        Ok(Next::Done((Some(node(3)), vec![record])))
    );
}

#[test]
fn ends_without_holders() {
    // Failed holders are passed over until none is left; a timeout ends the fetch at once.
    let family = Family::new([6u8; 32]);
    let mut fetch = fetch(&family);
    fetch.first().expect("first ask");
    let failed = || Err(PeerRequestError::Failed("offline".to_string()));
    fetch
        .next(answer(node(2), 1, failed()))
        .expect("second ask");
    assert_eq!(
        fetch.next(answer(node(3), 1, failed())),
        Ok(Next::Done((None, Vec::new())))
    );

    let mut fetch = super::FetchFamilyOperation::new(
        family.placement,
        vec![node(2), node(3)],
        (family.submission_id, family.request_digest),
        Duration::from_secs(10),
    );
    fetch.first().expect("first ask");
    assert_eq!(
        fetch.next(answer(node(2), 10, Err(PeerRequestError::TimedOut))),
        Ok(Next::Done((None, Vec::new())))
    );
}

#[test]
fn rejects_unfit_result() {
    let family = Family::new([7u8; 32]);
    let mut fetch = fetch(&family);
    fetch.first().expect("first ask");
    assert!(matches!(
        fetch.next(IoResult::Done),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}
