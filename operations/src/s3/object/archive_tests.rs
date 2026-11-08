//! Tests selection archives: folder paging, hidden and denied entries, pinned versions, path
//! conflicts, the report, ZIP64 and an archive reader that leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::DriverContext;
use crate::s3::object::list::ListedObject;
use crate::s3::object::pages::{ObjectPage, RangeCall};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use crate::tests::access::seed_access;
use crate::tests::staging::{create_test_bucket, setup_driver_context};
use aruna_core::NodeId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use std::io::Read;
use tokio::io::AsyncReadExt;

const BUCKET: &str = "selection";

/// Ordinary buckets only: no other member is ever asked.
struct NoRelay;

struct Listed(String);

impl From<&ListedObject> for Listed {
    fn from(object: &ListedObject) -> Self {
        Self(object.head.key.clone())
    }
}

impl PageObject for Listed {
    fn key(&self) -> &str {
        &self.0
    }
}

impl RangeRelay for NoRelay {
    type Object = Listed;
    type Error = ();

    async fn list_range(&self, _: RangeCall<'_>) -> Result<Option<ObjectPage<Listed>>, ()> {
        Ok(None)
    }

    fn is_fault(_: &()) -> bool {
        true
    }
}

impl MemberReader for NoRelay {
    async fn member_read(&self, _: MemberCall<'_>) -> Result<MemberRead, EntryStatus> {
        Ok(MemberRead::Local(None))
    }
}

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

    async fn archive<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        entries: Vec<SelectionEntry>,
        writer: W,
    ) -> Result<Report, SelectionError> {
        let scope = PageScope {
            context: self.context(),
            realm_id: self.realm_id,
            node_id: self.node_id,
            relay: &NoRelay,
        };
        write_selection(&scope, (&self.auth, u64::MAX), entries, writer).await
    }

    /// The archive's files by name, in archive order, and its report.
    async fn unzip(&self, entries: Vec<SelectionEntry>) -> (Vec<(String, Vec<u8>)>, Report) {
        let mut bytes = Vec::new();
        let report = self.archive(entries, &mut bytes).await.expect("archive");
        let files = unzip(&bytes);
        let (name, written) = files.last().expect("report entry");
        assert_eq!(name, REPORT_PATH);
        assert_eq!(serde_json::from_slice::<Report>(written).unwrap(), report);
        (files[..files.len() - 1].to_vec(), report)
    }
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
    // FOLDER_PAGE lists two keys a page, so the folder spans three pages.
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
    fixture.put("a.bin", &[7; 4096]).await;
    fixture.put("b.txt", b"old").await;
    let (writer, mut reader) = tokio::io::duplex(64);
    let entries = vec![file("a.bin", None), file("b.txt", None)];
    // The writer blocks inside the first entry until the reader drains it, so the second key is
    // resolved only after its newer version exists.
    let read = async {
        fixture.put("b.txt", b"new").await;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.expect("archive reads");
        bytes
    };
    let (written, bytes) = tokio::join!(fixture.archive(entries, writer), read);
    written.expect("archive");
    let files = unzip(&bytes);
    assert_eq!(files[1], ("selection/b.txt".to_string(), b"new".to_vec()));
}

#[tokio::test]
async fn zip64_entry_count() {
    let mut bytes = Vec::new();
    let mut zip = ZipFileWriter::with_tokio(&mut bytes);
    let count = usize::from(u16::MAX) + 2;
    for index in 0..count {
        let empty = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
        let path = format!("b/{index}");
        let size = write_entry(&mut zip, &path, 0, BackendStream::new(empty)).await;
        assert_eq!(size.expect("entry"), 0);
    }
    zip.close().await.expect("archive closes");
    let archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes)).expect("valid archive");
    assert_eq!(archive.len(), count);
}

#[tokio::test]
async fn closed_reader_stops() {
    let fixture = Fixture::new().await;
    fixture.put("a.csv", b"content").await;
    let (writer, reader) = tokio::io::duplex(16);
    drop(reader);
    let result = fixture.archive(vec![file("a.csv", None)], writer).await;
    assert!(matches!(result, Err(SelectionError::Write)));
}
