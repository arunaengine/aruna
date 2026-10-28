//! Tests the run link: written with the receipt, completed with the crate, kept past pruning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::job::{JobId, RunCrateStatus};
use ulid::Ulid;

use super::admission_race::{config, envelope, seed};
use crate::jobs::lifecycle::target::commit_receipt;
use crate::jobs::store::{
    batch_delete, prune_delete_entries, put_crate_outcome, read_crate_status, read_job_record,
    read_run_link,
};
use crate::tests::records::{Family, context};

#[tokio::test]
async fn run_link_lasts() {
    let family = Family::new([9u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let ctx = Arc::new(ctx);
    let (launch, _) = seed(&ctx, &family).await;
    let admitted = config(&family, &launch, 1, envelope(4));
    let (physical, logical) = (admitted.job_id, admitted.logical_job_id);
    commit_receipt(&ctx, admitted, &launch)
        .await
        .expect("offer is decided")
        .expect("admitted");
    let storage = &ctx.storage_handle;

    let link = read_run_link(storage, physical).await.unwrap().unwrap();
    assert_eq!(link.family, Some(family.family()));
    assert_eq!(link.logical_job_id, Some(logical));
    assert_eq!(link.run_crate, None);

    // Status and link land in one batch; before it, neither is final.
    assert_eq!(read_crate_status(storage, physical).await.unwrap(), None);
    let document_id = Ulid::from_bytes([7; 16]);
    let written = RunCrateStatus::Written {
        resource: document_id.to_string(),
    };
    let completed = aruna_core::structs::execution::job::RunLink {
        run_crate: Some(document_id),
        ..link
    };
    put_crate_outcome(storage, physical, &written, &completed)
        .await
        .unwrap();
    assert_eq!(
        read_crate_status(storage, physical).await.unwrap(),
        Some(written)
    );

    let record = read_job_record(storage, physical, None)
        .await
        .unwrap()
        .unwrap();
    batch_delete(storage, prune_delete_entries(&record), None)
        .await
        .unwrap();
    assert!(
        read_job_record(storage, physical, None)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(read_crate_status(storage, physical).await.unwrap(), None);
    let kept = read_run_link(storage, physical).await.unwrap();
    assert_eq!(kept, Some(completed));
    let unknown = read_run_link(storage, JobId::from_bytes([3; 16])).await;
    assert_eq!(unknown.unwrap(), None);
}
