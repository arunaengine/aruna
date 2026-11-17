//! Tests the sweep of secret marks: a mark stays while its work may write, goes once the version
//! is absent and the work ended, stays on a present version, and outlives the install's removal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use aruna_core::structs::execution::job::PluginWorkSpec;

use super::*;
use crate::driver::DriverContext;
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::jobs::retire_work;
use crate::plugins::plugin_work::tests::ready_copy;
use crate::plugins::rows::values_result;
use crate::plugins::rows::write_rows;
use crate::plugins::work::tests::edit_work;
use crate::plugins::worker::sweep_marks;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use aruna_core::calls::IoCall;

/// The current version of the copy's source object `in/a.txt`.
async fn source_version(ctx: &DriverContext) -> Ulid {
    let head = HeadObjectInput {
        bucket: "in".into(),
        key: "a.txt".into(),
        version_id: None,
    };
    let head = drive(HeadObjectOperation::new(head), ctx).await.unwrap();
    head.resolved_version_id.unwrap()
}

async fn mark(ctx: &DriverContext, version: Version<'_>, (install_id, work_id): (Ulid, Ulid)) {
    let mark = Mark {
        install_id,
        work_id,
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let marked = mark_row(version, &mark).unwrap();
    drive(write_rows(vec![marked]), ctx).await.unwrap();
}

fn work_of(spec: &PluginWorkSpec) -> (Ulid, Ulid) {
    (spec.install_id, spec.work_id)
}

#[tokio::test]
async fn sweep_awaits_work() {
    let fixture = Fixture::new().await;
    let (_root, blob, install, spec) = ready_copy(&fixture).await;
    let present = source_version(&blob).await;
    let (absent, orphan) = (Ulid::from_parts(3, 3), Ulid::from_parts(4, 4));
    mark(&blob, ("in", "a.txt", present), work_of(&spec)).await;
    mark(&blob, ("in", "a.txt", absent), work_of(&spec)).await;
    let unknown = (install.install_id, Ulid::from_parts(5, 5));
    mark(&blob, ("in", "b.txt", orphan), unknown).await;
    // The orphan's work is gone and its version absent; the copy's work may still write.
    assert_eq!(sweep_marks(&blob).await.unwrap(), 1);
    assert!(
        !drive(is_marked(("in", "b.txt", orphan)).unwrap(), &blob)
            .await
            .unwrap()
    );
    assert!(
        drive(is_marked(("in", "a.txt", absent)).unwrap(), &blob)
            .await
            .unwrap()
    );
    let settle = edit_work(&blob, work_of(&spec), |row| row.state = WorkState::Settled);
    settle.await.unwrap();
    assert_eq!(sweep_marks(&blob).await.unwrap(), 1);
    assert!(
        !drive(is_marked(("in", "a.txt", absent)).unwrap(), &blob)
            .await
            .unwrap()
    );
    assert!(
        drive(is_marked(("in", "a.txt", present)).unwrap(), &blob)
            .await
            .unwrap()
    );

    // Removing the install deletes its work rows, never its marks; a written version keeps its
    // mark for any later install.
    let mut config = read_config(&blob, fixture.actor.realm_id).await.unwrap();
    config.installs.clear();
    assert_eq!(retire_work(&blob, &config, 9).await.unwrap(), 1);
    assert_eq!(sweep_marks(&blob).await.unwrap(), 0);
    assert!(
        drive(is_marked(("in", "a.txt", present)).unwrap(), &blob)
            .await
            .unwrap()
    );
}

#[test]
fn mark_work_ended() {
    let mark = Mark {
        install_id: Ulid::from_parts(1, 1),
        work_id: Ulid::from_parts(2, 2),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let mut plan = MarkEnded {
        lookup: WorkLookup::new(mark.install_id, mark.work_id),
        key: Some(b"mark".to_vec()),
        deleted: false,
    };
    assert!(matches!(plan.first().unwrap(), Next::Call(IoCall::Read(_))));
    // A gone work row ends the mark, which goes in the same transaction.
    let delete = delete_call(vec![(PLUGIN_WORK_KEYSPACE, b"mark".to_vec())]);
    assert_eq!(
        plan.next(values_result(vec![None])).unwrap(),
        Next::Call(delete)
    );
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(true));
    let mut check = MarkEnded {
        lookup: WorkLookup::new(mark.install_id, mark.work_id),
        key: None,
        deleted: false,
    };
    assert_eq!(
        check.next(values_result(vec![None])).unwrap(),
        Next::Done(true)
    );
}
