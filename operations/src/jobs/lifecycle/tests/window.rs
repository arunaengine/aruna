//! Tests that a target declines a launch past its window or outside its membership interval.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::LaunchFrame;
use aruna_core::events::LaunchDecline;
use aruna_core::structs::execution::job::JobFamilyRecord;
use aruna_core::time::unix_timestamp_millis;

use super::admit_launch;
use super::terminal::{node_context, reserve_execution, seed_family};
use crate::tests::records::Family;

fn offer(family: &Family) -> LaunchFrame {
    let launch = family.launch(&family.spec(), family.holder.public(), 0);
    assert!(
        launch.expired(unix_timestamp_millis()),
        "the fixture launch is old"
    );
    let record = JobFamilyRecord::Launch(Box::new(launch));
    LaunchFrame::new(family.sign(&family.holder, record)).expect("bounded launch")
}

#[tokio::test]
async fn declines_expired_launch() {
    // Past the window on this target's clock no receipt is signed for the launch.
    let family = Family::new([60u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    seed_family(&ctx, &family).await;

    let answer = admit_launch(&ctx, offer(&family)).await;

    assert!(matches!(answer, Some(Err(LaunchDecline::Expired))));
}

#[tokio::test]
async fn replays_after_window() {
    // A launch the target already accepted still gets its receipt back after the window.
    let family = Family::new([61u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;

    let answer = admit_launch(&ctx, offer(&family)).await;

    let stored = answer.expect("replay decides").expect("receipt answers");
    assert!(matches!(
        &stored.envelope().record,
        JobFamilyRecord::Receipt(replayed) if replayed.execution_id == receipt.execution_id
    ));
}

#[tokio::test]
async fn declines_closed_interval() {
    // A fresh launch whose cited clock shows no open interval of this target is declined.
    let family = Family::new([62u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    seed_family(&ctx, &family).await;
    let mut launch = family.launch(&family.spec(), family.holder.public(), 1);
    launch.created_at_ms = unix_timestamp_millis();
    let record = JobFamilyRecord::Launch(Box::new(launch));
    let frame = LaunchFrame::new(family.sign(&family.holder, record)).expect("bounded launch");

    let answer = admit_launch(&ctx, frame).await;

    assert!(matches!(answer, Some(Err(LaunchDecline::WrongInterval))));
}
