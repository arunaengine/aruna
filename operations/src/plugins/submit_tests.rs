//! Tests starting copies from runs: a repeated call answers the same work and job, the same call
//! with other arguments conflicts, and refused paths or a paused install start nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;

use super::*;
use crate::driver::drive;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::calls::*;
#[allow(unused_imports)]
pub(crate) use crate::plugins::flow_support::submit::{fetcher, send, stored_object};
use crate::plugins::install::tests::Fixture;
use crate::plugins::rows::tests::child_result;
use crate::plugins::runs::tests::queued;
#[allow(unused_imports)]
pub(crate) use crate::plugins::test_support::{path, ready, request, scope};
use crate::plugins::work::read_work;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;

#[tokio::test]
async fn repeat_finds_work() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let scope = scope(&fixture, &install);
    let copy = request(&fixture, &install, "out/a.txt");
    let first = start_work(&fixture.ctx, &scope, WorkRequest::Copy(copy.clone()))
        .await
        .unwrap();
    let again = start_work(&fixture.ctx, &scope, WorkRequest::Copy(copy.clone()))
        .await
        .unwrap();
    assert_eq!(first, again);
    let row = drive(read_work(install.install_id, first), &fixture.ctx).await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.state, WorkState::Dispatched);
    assert!(row.job.is_some() && row.version_id.is_some());
    let paths = (row.source.as_deref(), row.target.as_deref());
    assert_eq!(
        paths,
        (Some(copy.source.as_str()), Some(copy.target.as_str()))
    );
    // The same call with other arguments conflicts; the run's next call starts its own copy.
    let mut other = request(&fixture, &install, "out/b.txt");
    let refused = start_work(&fixture.ctx, &scope, WorkRequest::Copy(other.clone())).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::Conflict)
    );
    other.ordinal = 1;
    let next = start_work(&fixture.ctx, &scope, WorkRequest::Copy(other))
        .await
        .unwrap();
    assert_ne!(next, first);
}

#[tokio::test]
async fn refusals_start_nothing() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let scope = scope(&fixture, &install);
    let kind = async |copy: CopyRequest| {
        let started = start_work(&fixture.ctx, &scope, WorkRequest::Copy(copy)).await;
        started.map_err(|error| error.kind)
    };
    let mut elsewhere = request(&fixture, &install, "out/a.txt");
    elsewhere.target = elsewhere.target.replace("/data/", "/meta/");
    assert_eq!(kind(elsewhere).await, Err(ErrorKind::NotGranted));
    let missing = request(&fixture, &install, "nowhere/a.txt");
    assert_eq!(kind(missing).await, Err(ErrorKind::NotFound));
    edit_config(&fixture, |config| config.installs[0].paused = true).await;
    let paused = request(&fixture, &install, "out/a.txt");
    assert_eq!(kind(paused).await, Err(ErrorKind::Denied));
}

#[tokio::test]
async fn read_source_suffices() {
    // A copy reads its source and writes its target: read on the source is enough, and the
    // target needs write.
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let (incoming, outgoing) = (
        path(&fixture, &install, "in/**"),
        path(&fixture, &install, "out/**"),
    );
    let choices = std::collections::BTreeMap::from([
        ("read".to_string(), vec![incoming.clone()]),
        ("write".to_string(), vec![outgoing]),
    ]);
    let (actor, auth) = (&fixture.actor, fixture.auth());
    set_grants(&fixture.ctx, actor, &auth, install.install_id, &choices)
        .await
        .unwrap();
    let scope = scope(&fixture, &install);
    let copy = request(&fixture, &install, "out/a.txt");
    assert!(
        start_work(&fixture.ctx, &scope, WorkRequest::Copy(copy))
            .await
            .is_ok()
    );
    let mut back = request(&fixture, &install, "in/b.txt");
    back.source = path(&fixture, &install, "out/a.txt");
    let refused = start_work(&fixture.ctx, &scope, WorkRequest::Copy(back)).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::NotGranted)
    );
}

fn fetch(fixture: &Fixture, install: &PluginInstall, secret: bool) -> DownloadRequest {
    DownloadRequest {
        context: RequestContext {
            connection: "catalog".into(),
            base: "https://api.example.org/api/".into(),
            method: "GET".into(),
            path: "reads.fq".into(),
            query: Vec::new(),
            headers: Vec::new(),
        },
        target: path(fixture, install, "in/reads.fq"),
        checksums: Vec::new(),
        max_bytes: None,
        secret,
        ordinal: 0,
    }
}

/// The marks of the work keyspace, by their key.
async fn marks(fixture: &Fixture) -> usize {
    use crate::plugins::marks::MARK_ROW;
    use crate::plugins::store::Txn;
    let txn = Txn::direct(&fixture.ctx);
    let rows = txn.scan(PLUGIN_WORK_KEYSPACE, &[MARK_ROW]).await.unwrap();
    rows.len()
}

#[tokio::test]
async fn download_marked() {
    // A download through a connection with a secret marks the version it will write in the
    // reservation, before any byte exists; the same call again finds the same work and mark.
    use crate::plugins::marks::is_marked;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let scope = scope(&fixture, &install);
    let request = fetch(&fixture, &install, true);
    let work_id = start_work(&fixture.ctx, &scope, WorkRequest::Download(request.clone()))
        .await
        .unwrap();
    let row = drive(read_work(install.install_id, work_id), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    let version = ("in", "reads.fq", row.version_id.unwrap());
    assert!(
        drive(is_marked(version).unwrap(), &fixture.ctx)
            .await
            .unwrap()
    );
    let again = start_work(&fixture.ctx, &scope, WorkRequest::Download(request.clone())).await;
    assert_eq!(again, Ok(work_id));
    assert_eq!(marks(&fixture).await, 1);
    let mut other = request;
    other.max_bytes = Some(5);
    let changed = start_work(&fixture.ctx, &scope, WorkRequest::Download(other))
        .await
        .unwrap_err();
    assert_eq!(changed.kind, ErrorKind::Conflict);
}

#[tokio::test]
async fn download_unmarked() {
    // Without a secret the answer is the service's public data, so nothing is marked.
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let scope = scope(&fixture, &install);
    start_work(
        &fixture.ctx,
        &scope,
        WorkRequest::Download(fetch(&fixture, &install, false)),
    )
    .await
    .unwrap();
    assert_eq!(marks(&fixture).await, 0);
}

#[tokio::test]
async fn marks_once() {
    // Two reservations of the same call at once, the loser tried again, leave one mark.
    use crate::plugins::work::Reserve;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let identity = Identity::Call {
        run_id: Ulid::from_parts(9, 9),
        ordinal: 0,
    };
    let proposed = || {
        let mut row = WorkRow::reserved(&install, WorkKind::Download, Ulid::from_parts(9, 9), 1);
        (row.digest, row.version_id) = ([3; 32], Some(Ulid::generate()));
        row
    };
    let marked = Some(("in", "reads.fq"));
    let reserve = async |row| {
        drive(
            reserve((&install, &identity), row, marked).unwrap(),
            &fixture.ctx,
        )
        .await
    };
    let (first, second) = tokio::join!(reserve(proposed()), reserve(proposed()));
    let mut found = Vec::new();
    for outcome in [first, second] {
        let outcome = match outcome {
            Ok(found) => found,
            Err(_) => reserve(proposed()).await.unwrap(),
        };
        found.push(outcome);
    }
    let fresh = found
        .iter()
        .filter(|(_, how)| *how == Reserve::Fresh)
        .count();
    assert_eq!(fresh, 1, "one reservation wrote the row");
    assert_eq!(found[0].0.work_id, found[1].0.work_id);
    assert_eq!(marks(&fixture).await, 1);
}

#[tokio::test]
async fn retired_unmarked() {
    // A download that was never submitted, retired with its install, loses its mark at the next
    // sweep: its row is gone and the version it would have written never existed.
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let identity = Identity::Call {
        run_id: Ulid::from_parts(9, 9),
        ordinal: 0,
    };
    let mut row = WorkRow::reserved(&install, WorkKind::Download, Ulid::from_parts(9, 9), 1);
    row.version_id = Some(Ulid::generate());
    let marked = Some(("in", "reads.fq"));
    drive(
        reserve((&install, &identity), row, marked).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        sweep_marks(&fixture.ctx).await,
        Ok(0),
        "its work may still write"
    );
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Removing
    })
    .await;
    let config = crate::plugins::calls::read_config(&fixture.ctx, fixture.actor.realm_id);
    retire_work(&fixture.ctx, &config.await.unwrap(), 5)
        .await
        .unwrap();
    assert_eq!(sweep_marks(&fixture.ctx).await, Ok(1));
    assert_eq!(marks(&fixture).await, 0);
}

#[tokio::test]
async fn upload_started() {
    // An upload needs a readable, unmarked source; the same call finds the same work.
    use crate::plugins::marks::{Mark, mark_row};
    use crate::plugins::objects::Unmarked;
    use crate::plugins::rows::write_rows;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let (_root, blob) = stored_object(&fixture, &install).await;
    let scope = scope(&fixture, &install);
    let work_id = start_work(
        &blob,
        &scope,
        WorkRequest::Upload(send(&fixture, &install, None)),
    )
    .await
    .unwrap();
    assert_eq!(
        start_work(
            &blob,
            &scope,
            WorkRequest::Upload(send(&fixture, &install, None))
        )
        .await,
        Ok(work_id)
    );
    let version = drive(Unmarked::new(("in", "a.txt"), None), &blob)
        .await
        .unwrap()
        .unwrap();
    let pinned = start_work(
        &blob,
        &scope,
        WorkRequest::Upload(send(&fixture, &install, Some(version))),
    )
    .await;
    assert_eq!(pinned.unwrap_err().kind, ErrorKind::Conflict);
    let mut missing = send(&fixture, &install, None);
    (missing.source, missing.ordinal) = (path(&fixture, &install, "in/none"), 1);
    assert_eq!(
        start_work(&blob, &scope, WorkRequest::Upload(missing))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    let mark = Mark {
        install_id: Ulid::from_parts(8, 8),
        work_id: Ulid::from_parts(8, 9),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let marked = mark_row(("in", "a.txt", version), &mark).unwrap();
    drive(write_rows(vec![marked]), &blob).await.unwrap();
    let mut later = send(&fixture, &install, None);
    later.ordinal = 2;
    assert_eq!(
        start_work(&blob, &scope, WorkRequest::Upload(later))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Denied
    );
}

fn plain_scope(install: &PluginInstall) -> RunScope {
    RunScope {
        realm_id: install.account.realm_id,
        local: install.receiver,
        run: queued(install.install_id, 1),
    }
}

fn plain_copy(install: &PluginInstall) -> WorkRequest {
    let realm = install.account.realm_id;
    let root = format!("/{realm}/g/{}/data/{}", install.group_id, install.receiver);
    WorkRequest::Copy(CopyRequest {
        source: format!("{root}/b/a"),
        source_version: None,
        target: format!("{root}/b/c"),
        ordinal: 0,
    })
}

fn checked(install: &PluginInstall) -> IoResult {
    child_result(Ok::<_, CallError>(install.clone()))
}

fn placed() -> IoResult {
    child_result(Ok::<_, CallError>(("b".to_string(), "a".to_string())))
}

fn is_child(next: Next<PreparedWork>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

#[test]
fn copy_prepared_ordered() {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let scope = plain_scope(&install);
    let mut prepare = PrepareWork::new(&scope, plain_copy(&install), (5, Ulid::from_parts(8, 8)));
    assert!(is_child(prepare.first().unwrap()));
    assert!(is_child(prepare.next(checked(&install)).unwrap()));
    assert_eq!(prepare.current, Step::CopyGrants);
    let grants = child_result(Ok::<_, CallError>(BTreeSet::<String>::new()));
    assert!(is_child(prepare.next(grants).unwrap()));
    assert_eq!(prepare.current, Step::Source);
    assert!(is_child(prepare.next(placed()).unwrap()));
    assert_eq!(prepare.current, Step::SourceVersion);
    let absent = child_result(Ok::<Option<Ulid>, CallError>(None));
    assert!(is_child(prepare.next(absent).unwrap()), "a copy goes on");
    assert_eq!(prepare.current, Step::Target);
    assert!(is_child(prepare.next(placed()).unwrap()));
    assert_eq!(prepare.current, Step::Reserve);
    let mut row = WorkRow::reserved(&install, WorkKind::Copy, Ulid::nil(), 1);
    row.version_id = Some(Ulid::from_parts(8, 8));
    let reserved = child_result(Ok::<_, WorkError>((row.clone(), Reserve::Fresh)));
    let Next::Done(PreparedWork::Submit { account, spec }) = prepare.next(reserved).unwrap() else {
        panic!("a reserved row is submitted");
    };
    assert_eq!((account, spec.work_id), (install.account, row.work_id));
}

#[test]
fn known_work_answers() {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let scope = plain_scope(&install);
    let mut prepare = PrepareWork::new(&scope, plain_copy(&install), (5, Ulid::nil()));
    prepare.install = Some(install.clone());
    prepare.current = Step::Reserve;
    let mut row = WorkRow::reserved(&install, WorkKind::Copy, Ulid::nil(), 1);
    row.state = WorkState::Dispatched;
    let found = child_result(Ok::<_, WorkError>((row.clone(), Reserve::Earlier)));
    let answer = prepare.next(found);
    assert_eq!(answer, Ok(Next::Done(PreparedWork::Known(row.work_id))));
    let mut conflict = PrepareWork::new(&scope, plain_copy(&install), (5, Ulid::nil()));
    conflict.current = Step::Reserve;
    let refused = conflict.next(child_result(Err::<(WorkRow, Reserve), _>(
        WorkError::Conflict,
    )));
    assert_eq!(refused.unwrap_err().0.kind, ErrorKind::Conflict);
}

#[test]
fn paused_starts_nothing() {
    let mut install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    install.paused = true;
    let scope = plain_scope(&install);
    let mut prepare = PrepareWork::new(&scope, plain_copy(&install), (5, Ulid::nil()));
    prepare.next(checked(&install)).unwrap();
    let grants = child_result(Ok::<_, CallError>(BTreeSet::<String>::new()));
    assert_eq!(prepare.next(grants).unwrap_err().0.kind, ErrorKind::Denied);
}

#[test]
fn missing_upload_source() {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let scope = plain_scope(&install);
    let request = WorkRequest::Upload(UploadRequest {
        context: RequestContext {
            connection: "api".into(),
            base: "https://api.example.org".into(),
            method: "PUT".into(),
            path: "x".into(),
            query: Vec::new(),
            headers: Vec::new(),
        },
        source: "/s".into(),
        version: None,
        form: None,
        ordinal: 0,
    });
    let mut prepare = PrepareWork::new(&scope, request, (5, Ulid::nil()));
    prepare.current = Step::SourceVersion;
    let absent = child_result(Ok::<Option<Ulid>, CallError>(None));
    assert_eq!(
        prepare.next(absent).unwrap_err().0.kind,
        ErrorKind::NotFound
    );
}
