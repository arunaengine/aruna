//! Tests work rows: a repeated call adopts the row, other arguments conflict, a reinstall adopts
//! nothing, the index finds the row, and dispatch records the job once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::worker::tests::installed;

fn proposed(install: &PluginInstall, digest: u8) -> WorkRow {
    let mut row = WorkRow::reserved(install, WorkKind::Copy, Ulid::generate(), 1);
    row.digest = [digest; 32];
    row.version_id = Some(Ulid::generate());
    row
}

fn job(nonce: u64) -> JobId {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    JobId::from_parts(1_700_000_000_000, handle, BucketId::new(0).unwrap(), nonce).unwrap()
}

#[tokio::test]
async fn repeated_call_adopts() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = Identity::Key("copy-1".into());
    let first = proposed(&install, 1);
    let reserved = reserve(&fixture.ctx, (&install, &identity), first.clone()).await;
    assert_eq!(reserved, Ok((first.clone(), Reserve::Fresh)));
    // A repeat mints new ids, but the stored ones answer.
    let again = reserve(&fixture.ctx, (&install, &identity), proposed(&install, 1)).await;
    assert_eq!(again, Ok((first.clone(), Reserve::Earlier)));
    let other = reserve(&fixture.ctx, (&install, &identity), proposed(&install, 2)).await;
    assert_eq!(other, Err(WorkError::Conflict));
    let call = Identity::Call {
        run_id: first.run_id,
        ordinal: 3,
    };
    let unkeyed = reserve(&fixture.ctx, (&install, &call), proposed(&install, 2)).await;
    assert_eq!(unkeyed.map(|(_, reserve)| reserve), Ok(Reserve::Fresh));
    let found = read_work(&fixture.ctx, install.install_id, first.work_id).await;
    assert_eq!(found, Ok(Some(first)));
    let missing = read_work(&fixture.ctx, install.install_id, Ulid::generate()).await;
    assert_eq!(missing, Ok(None));
}

#[tokio::test]
async fn reinstall_adopts_nothing() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = Identity::Key("copy-1".into());
    let first = proposed(&install, 1);
    reserve(&fixture.ctx, (&install, &identity), first.clone())
        .await
        .unwrap();
    let mut reinstalled = install.clone();
    reinstalled.account = UserId::local(Ulid::generate(), install.account.realm_id);
    let fresh = proposed(&reinstalled, 1);
    let reserved = reserve(&fixture.ctx, (&reinstalled, &identity), fresh.clone()).await;
    assert_eq!(reserved, Ok((fresh, Reserve::Fresh)));
}

#[tokio::test]
async fn dispatch_records_once() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = Identity::Key("copy-1".into());
    reserve(&fixture.ctx, (&install, &identity), row.clone())
        .await
        .unwrap();
    let ids = (install.install_id, row.work_id);
    let first = dispatched(&fixture.ctx, ids, job(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first.state, first.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    let later = dispatched(&fixture.ctx, ids, job(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later.job, Some(job(1)));
    let gone = dispatched(&fixture.ctx, (install.install_id, Ulid::generate()), job(3)).await;
    assert_eq!(gone, Ok(None));
}
