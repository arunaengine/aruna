//! Call and result tests of one launch offer to a remote execution target.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{
    Effect, JobRecordFrame, LaunchFrame, MetadataRequestEffect, NetEffect, ReceiptFrame,
};
use aruna_core::events::{Event, LaunchDecline, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobFamilyRecord;

use super::{OfferLaunchOperation, OfferOutcome};
use crate::jobs::lifecycle::LifecycleError;
use crate::metadata::protocol::{MetadataTransportMessage, encode_message};
use crate::tests::records::{Family, node};

fn launch(family: &Family) -> LaunchFrame {
    let intent = family.launch(&family.spec(), family.holder.public(), 0);
    let envelope = family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(intent)));
    LaunchFrame::new(envelope).expect("bounded launch")
}

fn receipt(family: &Family) -> ReceiptFrame {
    let intent = family.launch(&family.spec(), family.holder.public(), 0);
    let receipt = family.receipt(&intent, 1);
    let envelope = family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)));
    ReceiptFrame::new(envelope).expect("bounded receipt")
}

fn answer(result: Result<MetadataTransportMessage, PeerRequestError>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        MetadataAnswer {
            peer: node(4),
            elapsed: Duration::from_secs(1),
            result: result.map(|message| encode_message(&message).expect("reply encodes")),
        },
    )))))
}

fn offer(family: &Family) -> OfferLaunchOperation {
    OfferLaunchOperation::new(node(4), launch(family), Duration::from_secs(30))
}

#[test]
fn accepts_receipt() {
    let family = Family::new([8u8; 32]);
    let mut offer = offer(&family);
    let frame = encode_message(&MetadataTransportMessage::ForwardLaunchOffer {
        launch: Box::new(launch(&family)),
    })
    .expect("frame encodes");
    assert_eq!(
        offer.first(),
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(
            NetEffect::MetadataRequest(Box::new(MetadataRequestEffect {
                peer: node(4),
                frame,
                timeout: Duration::from_secs(30),
            }))
        )))))
    );
    let receipt = receipt(&family);
    let expected = JobRecordFrame::new(receipt.clone().into_inner()).expect("receipt record");
    let reply = MetadataTransportMessage::ForwardedLaunchOffer {
        result: Ok(Box::new(receipt)),
    };
    assert_eq!(
        offer.next(answer(Ok(reply))),
        Ok(Next::Done(OfferOutcome::Accepted(expected)))
    );
}

#[test]
fn reports_declines() {
    let family = Family::new([9u8; 32]);
    let mut declined = offer(&family);
    declined.first().expect("offer");
    let reply = MetadataTransportMessage::ForwardedLaunchOffer {
        result: Err(LaunchDecline::Cancelled),
    };
    assert_eq!(
        declined.next(answer(Ok(reply))),
        Ok(Next::Done(OfferOutcome::Declined(LaunchDecline::Cancelled)))
    );

    // A target that never answers may still have accepted the launch.
    let mut silent = offer(&family);
    silent.first().expect("offer");
    assert_eq!(
        silent.next(answer(Err(PeerRequestError::TimedOut))),
        Ok(Next::Done(OfferOutcome::Unavailable))
    );
}

#[test]
fn rejects_unfit_result() {
    let family = Family::new([10u8; 32]);
    let mut offer = offer(&family);
    offer.first().expect("offer");
    assert!(matches!(
        offer.next(IoResult::Done),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}
