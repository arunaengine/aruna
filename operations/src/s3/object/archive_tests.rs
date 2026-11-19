//! Tests selection archives: folder paging, hidden and denied entries, pinned versions, path
//! conflicts, the report, ZIP64 and an archive reader that leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use crate::tests::access::seed_access;
use crate::tests::staging::{create_test_bucket, setup_driver_context};
use aruna_blob::archive::{open_zip, zip_close, zip_counted};
use aruna_core::NodeId;
use aruna_core::calls::ChildOutcome;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use futures_util::StreamExt;
use std::io::Read;

const BUCKET: &str = "selection";

fn file(key: &str, version_id: Option<Ulid>) -> SelectionEntry {
    SelectionEntry::File {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        version_id,
    }
}

fn folder(prefix: &str) -> SelectionEntry {
    SelectionEntry::Folder {
        bucket: BUCKET.to_string(),
        prefix: prefix.to_string(),
    }
}

struct Fixture {
    staging: crate::tests::staging::StagingTestContext,
    auth: AuthContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: GroupId,
}

impl Fixture {
    async fn new() -> Self {
        let staging = setup_driver_context().await;
        let context = &staging.driver_context;
        let realm_id = RealmId::from_bytes([2; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let group_id = Ulid::from_parts(2, 2);
        let denied = object_permission_path(realm_id, group_id, node_id, BUCKET, "secret/**");
        let auth = seed_access(context, (realm_id, group_id, node_id), &denied).await;
        create_test_bucket(context, group_id, auth.user_id, BUCKET).await;
        Self {
            staging,
            auth,
            realm_id,
            node_id,
            group_id,
        }
    }

    fn context(&self) -> &DriverContext {
        &self.staging.driver_context
    }

    async fn put(&self, key: &str, body: &[u8]) -> Ulid {
        let body = bytes::Bytes::copy_from_slice(body);
        let length = body.len() as u64;
        let stream = futures_util::stream::iter([Ok::<_, std::io::Error>(body)]);
        let operation = PutObjectOperation::new(PutObjectConfig {
            user_id: self.auth.user_id,
            group_id: self.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(length),
                body: Some(BackendStream::new(stream)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(self.group_id),
        });
        drive(operation, self.context())
            .await
            .expect("put drives")
            .version_id
    }

    async fn archive(
        &self,
        entries: Vec<SelectionEntry>,
        zip: ZipWork,
    ) -> Result<Report, SelectionError> {
        let node = PageNode {
            realm_id: self.realm_id,
            node_id: self.node_id,
            ids: self.context().node_and_realm(),
        };
        let access = MemberAccess::Bearer(None);
        let caller = (&self.auth, u64::MAX);
        let mut write = WriteSelectionOperation::new(zip, node, access, caller, entries);
        // Small pages, so a folder spans several.
        write.folder_page = 2;
        drive(write, self.context()).await
    }

    /// The archive's files by name, in archive order, and its report.
    async fn unzip(&self, entries: Vec<SelectionEntry>) -> (Vec<(String, Vec<u8>)>, Report) {
        let (zip, stream) = open_zip();
        let (report, bytes) = tokio::join!(self.archive(entries, zip), collect(stream));
        let report = report.expect("archive");
        let files = unzip(&bytes);
        let (name, written) = files.last().expect("report entry");
        assert_eq!(name, REPORT_PATH);
        assert_eq!(serde_json::from_slice::<Report>(written).unwrap(), report);
        (files[..files.len() - 1].to_vec(), report)
    }
}

/// The bytes of an archive stream to its end.
async fn collect(mut stream: ByteStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.expect("archive reads"));
    }
    bytes
}

fn unzip(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid archive");
    (0..archive.len())
        .map(|index| {
            let mut file = archive.by_index(index).expect("entry");
            let mut content = Vec::new();
            file.read_to_end(&mut content).expect("entry reads");
            (file.name().expect("utf-8 name").to_string(), content)
        })
        .collect()
}

fn names(files: &[(String, Vec<u8>)]) -> Vec<&str> {
    files.iter().map(|(name, _)| name.as_str()).collect()
}

fn row(key: &str, version: Option<Ulid>, status: EntryStatus) -> ReportRow {
    file_row(BUCKET, key, version, status)
}

#[test]
fn selection_is_checked() {
    assert!(matches!(
        validate_selection(&[]),
        Err(SelectionError::Empty)
    ));
    let bad = [SelectionEntry::Folder {
        bucket: "_x".to_string(),
        prefix: String::new(),
    }];
    assert!(matches!(
        validate_selection(&bad),
        Err(SelectionError::Bucket(_))
    ));
    assert!(validate_selection(&[file("a", None), file("a", None)]).is_ok());
}

#[test]
fn paths_stay_apart() {
    let version = Ulid::from_bytes([7; 16]);
    let renamed = format!("_renamed/b/{version}");
    let mut paths = WrittenPaths::default();
    for unsafe_key in ["../x", "a/", "/a", "a//b", "a\\b"] {
        assert_eq!(
            paths.place("b", unsafe_key, version),
            (renamed.clone(), true)
        );
    }
    let placed = paths.place("b", "./a", version);
    assert_eq!(placed, ("b/a".to_string(), false));
    paths.record(&placed.0);
    // An alias of a written path, and a path below a written file.
    assert_eq!(paths.place("b", "a", version), (renamed.clone(), true));
    assert_eq!(paths.place("b", "a/c", version), (renamed.clone(), true));
    assert_eq!(paths.place("c", "a", version), ("c/a".to_string(), false));
    // A path equal to a written path's folder.
    paths.record("b/x/y/z");
    assert_eq!(paths.place("b", "x/y", version), (renamed.clone(), true));
    assert_eq!(paths.place("b", "x", version), (renamed, true));
    assert_eq!(
        paths.place("b", "x/q", version),
        ("b/x/q".to_string(), false)
    );
}

#[test]
fn locked_key_status() {
    let locked = ConversionError::BucketKey(BucketKeyError::Locked(Ulid::nil()));
    assert_eq!(
        read_status(&GetObjectError::ConversionError(locked)),
        EntryStatus::Locked
    );
    assert_eq!(
        read_status(&GetObjectError::HistoricalReferenceUnavailable),
        EntryStatus::Unavailable
    );
    assert_eq!(
        read_status(&GetObjectError::ReferenceChecksumMismatch),
        EntryStatus::Unavailable
    );
}

#[tokio::test]
async fn folders_page_through() {
    let fixture = Fixture::new().await;
    for index in 1..=5 {
        fixture.put(&format!("data/{index}"), b"x").await;
    }
    fixture.put("other.txt", b"o").await;
    // The test lists two keys a page, so the folder spans three pages.
    let entries = vec![
        file("other.txt", None),
        folder("data/"),
        file("data/3", None),
    ];
    let (files, report) = fixture.unzip(entries).await;
    assert_eq!(
        names(&files),
        [
            "selection/data/1",
            "selection/data/2",
            "selection/data/3",
            "selection/data/4",
            "selection/data/5",
            "selection/other.txt"
        ]
    );
    assert!(report.entries.is_empty());
    assert_eq!(report.totals.included, 6);
    assert_eq!(report.totals.bytes, 6);
}

#[tokio::test]
async fn denied_folder_row() {
    let fixture = Fixture::new().await;
    fixture.put("open/a", b"a").await;
    fixture.put("secret/b", b"b").await;
    fixture.put("secret/c", b"c").await;
    let (files, report) = fixture.unzip(vec![folder("secret/"), folder("")]).await;
    // The whole bucket hides the denied keys without a row; the denied folder is one row.
    assert_eq!(names(&files), ["selection/open/a"]);
    let denied = ReportRow {
        status: EntryStatus::Denied,
        ..folder_row(BUCKET, "secret/", None)
    };
    assert_eq!(report.entries, [denied]);
    assert_eq!((report.totals.denied, report.totals.included), (1, 1));
    // An explicit file below a denied folder is checked on its own.
    let (files, report) = fixture.unzip(vec![file("secret/b", None)]).await;
    assert!(files.is_empty());
    assert_eq!(report.entries, [row("secret/b", None, EntryStatus::Denied)]);
}

#[tokio::test]
async fn versions_and_duplicates() {
    let fixture = Fixture::new().await;
    let old = fixture.put("a.csv", b"first").await;
    let current = fixture.put("a.csv", b"second").await;
    let entries = vec![
        file("a.csv", Some(old)),
        folder(""),
        file("a.csv", None),
        file("a.csv", Some(current)),
        file("a.csv", Some(old)),
        file("absent.csv", None),
    ];
    let (files, report) = fixture.unzip(entries).await;
    let renamed = format!("_renamed/selection/{old}");
    assert_eq!(names(&files), ["selection/a.csv", renamed.as_str()]);
    assert_eq!(files[0].1, b"second");
    assert_eq!(files[1].1, b"first");
    let moved = ReportRow {
        path: Some(renamed),
        ..row("a.csv", Some(old), EntryStatus::Renamed)
    };
    let absent = row("absent.csv", None, EntryStatus::Missing);
    assert_eq!(report.entries, [moved, absent]);
    let totals = &report.totals;
    assert_eq!((totals.included, totals.renamed, totals.missing), (1, 1, 1));
    assert_eq!(totals.bytes, 11);
}

#[tokio::test]
async fn conflicting_keys_renamed() {
    let fixture = Fixture::new().await;
    let alias = fixture.put("./a", b"1").await;
    let plain = fixture.put("a", b"2").await;
    let below = fixture.put("a/b", b"3").await;
    let folder_key = fixture.put("a/", b"4").await;
    let (files, report) = fixture.unzip(vec![folder("")]).await;
    let renamed = |version: Ulid| format!("_renamed/selection/{version}");
    let expected = [
        "selection/a".to_string(),
        renamed(plain),
        renamed(folder_key),
        renamed(below),
    ];
    assert_eq!(names(&files), expected.each_ref().map(String::as_str));
    assert_eq!(
        files[0].1, b"1",
        "the first key in key order keeps the path"
    );
    assert_eq!(report.totals.renamed, 3);
    assert!(
        report
            .entries
            .iter()
            .all(|row| row.status == EntryStatus::Renamed)
    );
    let (again, _) = fixture.unzip(vec![folder("")]).await;
    assert_eq!(names(&again), names(&files), "the paths are deterministic");
    assert_ne!(alias, plain);
}

#[tokio::test]
async fn versions_pinned_late() {
    let fixture = Fixture::new().await;
    // Larger than the archive pipe, so the writer blocks inside this entry.
    fixture.put("a.bin", &vec![7; 1024 * 1024]).await;
    fixture.put("b.txt", b"old").await;
    let (zip, stream) = open_zip();
    let entries = vec![file("a.bin", None), file("b.txt", None)];
    // The second key is resolved only after the reader drains the first, so after its new version.
    let read = async {
        fixture.put("b.txt", b"new").await;
        collect(stream).await
    };
    let (written, bytes) = tokio::join!(fixture.archive(entries, zip), read);
    written.expect("archive");
    let files = unzip(&bytes);
    assert_eq!(files[1], ("selection/b.txt".to_string(), b"new".to_vec()));
}

#[tokio::test]
async fn zip64_entry_count() {
    let (zip, stream) = open_zip();
    let count = usize::from(u16::MAX) + 2;
    let write = async {
        let mut zip = zip;
        for index in 0..count {
            let empty = futures_util::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
            let path = format!("b/{index}");
            let (next, size) = zip_counted(zip, &path, 0, BackendStream::new(empty)).await;
            assert_eq!(size, Ok(0));
            zip = next.expect("the writer stays");
        }
        zip_close(zip).await.expect("archive closes");
    };
    let (_, bytes) = tokio::join!(write, collect(stream));
    let archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes)).expect("valid archive");
    assert_eq!(archive.len(), count);
}

#[tokio::test]
async fn closed_reader_stops() {
    let fixture = Fixture::new().await;
    fixture.put("a.csv", b"content").await;
    let (zip, stream) = open_zip();
    drop(stream);
    let result = fixture.archive(vec![file("a.csv", None)], zip).await;
    assert_eq!(result, Err(SelectionError::Write));
}

fn no_zip() -> ZipWork {
    ZipWork::new(Box::new(()))
}

fn step_node() -> PageNode {
    PageNode {
        realm_id: RealmId::from_bytes([2; 32]),
        node_id: iroh::SecretKey::from_bytes(&[6; 32]).public(),
        ids: None,
    }
}

fn step_write(entries: Vec<SelectionEntry>) -> WriteSelectionOperation {
    let node = step_node();
    let auth = AuthContext::anonymous(node.realm_id);
    let access = MemberAccess::Bearer(None);
    WriteSelectionOperation::new(no_zip(), node, access, (&auth, 0), entries)
}

fn job_answer(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

#[test]
fn open_answers_writer() {
    let mut open = OpenArchiveOperation;
    let expected = IoCall::from(Effect::Job(JobEffect::OpenZip));
    assert_eq!(open.first(), Ok(Next::Call(expected)));
    let empty = futures_util::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
    let opened = JobEvent::ZipOpened {
        zip: no_zip(),
        stream: BackendStream::new(empty),
    };
    assert!(matches!(open.next(job_answer(opened)), Ok(Next::Done(_))));
}

#[test]
fn empty_selection_refused() {
    assert_eq!(step_write(Vec::new()).first(), Err(SelectionError::Empty));
}

#[test]
fn missing_bucket_reported() {
    let mut write = step_write(vec![file("a", None)]);
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = Err::<BucketInfo, _>(GetBucketError::NotFound);
    let report = write.next(IoResult::Child(ChildOutcome::new(missing)));
    assert!(matches!(report, Ok(Next::Call(IoCall::Effect(_)))));
    let written = JobEvent::ZipWritten {
        zip: Some(no_zip()),
        result: Ok(()),
    };
    assert!(matches!(
        write.next(job_answer(written)),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let Ok(Next::Done(report)) = write.next(job_answer(JobEvent::ZipClosed(Ok(())))) else {
        panic!("the archive ends");
    };
    assert_eq!(report.entries, [row("a", None, EntryStatus::Missing)]);
}

#[test]
fn failed_close_refused() {
    let mut write = step_write(vec![file("a", None)]);
    write.step = ArchiveStep::Close;
    let closed = JobEvent::ZipClosed(Err("gone".to_string()));
    assert_eq!(write.next(job_answer(closed)), Err(SelectionError::Write));
}

fn step_resolve() -> ResolveKeyOperation {
    let node = step_node();
    let auth = AuthContext::anonymous(node.realm_id);
    let info = BucketInfo {
        group_id: Ulid::from_parts(2, 2),
        created_at: std::time::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: aruna_core::structs::storage::format::Compression::Off,
        distribution: None,
    };
    ResolveKeyOperation::new(node, (&auth, 0), (BUCKET, &info), "a", None)
}

#[test]
fn denied_key_unread() {
    let mut resolve = step_resolve();
    resolve.first().unwrap();
    let denied = Err::<(), _>(AuthorizeError::PermissionDenied);
    let answer = resolve.next(IoResult::Child(ChildOutcome::new(denied)));
    assert_eq!(answer, Err(EntryStatus::Denied));
}
