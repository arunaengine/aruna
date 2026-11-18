//! Tests of the backend import: its pure decisions and its reads against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::reads::{
    self, CandidatesOperation, ClaimsOperation, CommittedMd5Operation, HideAliasOperation,
    StoredIntentsOperation, TrailOperation, exact_of, version_row,
};
use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::blob_request::BlobRequest;
use crate::jobs::import_listing::{ListingNote, NoteError, NoteListingOperation};
use crate::jobs::page_intents::PageIntentsOperation;
use aruna_core::calls::{IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_TRAIL_KEYSPACE,
    IMPORT_INTENT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::backend_import::intent_key;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer, VersionKey};
use aruna_core::structs::storage::format::EncodingClass;

#[test]
fn mapped_discovery_owned() {
    use aruna_core::structs::storage::backend_import::PLAIN_PATTERN;
    use aruna_core::structs::storage::format::EncodingClass;
    let mut rule = BackendImport {
        import_id: Ulid::nil(),
        group_id: Ulid::nil(),
        backend_id: Ulid::nil(),
        backend_prefix: "team-a-x/".to_string(),
        bucket: "x".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: PLAIN_PATTERN.to_string(),
    };
    let location = BackendLocation {
        backend: BackendRef::Group(Ulid::nil()),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: "team-a-x/a.csv".to_string(),
        ulid: Ulid::from_parts(7, 7),
        format: Default::default(),
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: Default::default(),
    };
    let owned = discovered_copy(&rule, location.clone()).format.encoding();
    assert_eq!(
        owned,
        EncodingClass::Plain {
            location: location.ulid
        }
    );
    // An ordinary rule keeps sharing identical bytes on its backend.
    rule.write_pattern = "{dir}{id}_{name}".to_string();
    let shared = discovered_copy(&rule, location).format.encoding();
    assert_eq!(shared, EncodingClass::Raw);
}

fn object(etag: &str) -> ExternalObject {
    ExternalObject {
        key: "raw/a.csv".to_string(),
        size: 4,
        etag: Some(etag.to_string()),
        modified_ms: None,
    }
}

fn row(version_id: Ulid, etag: &str) -> ImportedObject {
    ImportedObject::new(version_id, &object(etag))
}

fn listing() -> BlobEffect {
    BlobEffect::ListExternal {
        backend: BackendRef::Group(Ulid::nil()),
        prefix: "raw/".to_string(),
        start_after: None,
        limit: 1,
    }
}

fn listed() -> IoResult {
    IoResult::Event(Box::new(Event::Blob(BlobEvent::ExternalListed {
        objects: Vec::new(),
        next: None,
    })))
}

#[test]
fn checks_blob_answers() {
    let mut request = BlobRequest::new(listing());
    assert_eq!(
        request.first(),
        Ok(Next::Call(IoCall::Effect(Box::new(
            Effect::Blob(listing())
        ))))
    );
    assert!(matches!(
        request.next(listed()),
        Ok(Next::Done(BlobEvent::ExternalListed { .. }))
    ));

    // Another kind of answer does not end a listing.
    let mut request = BlobRequest::new(listing());
    request.first().expect("listing call");
    let deleted = IoResult::Event(Box::new(Event::Blob(BlobEvent::DeleteFinished)));
    assert!(request.next(deleted).is_err());

    // A request is sent once, and an answer after its answer is refused.
    let mut request = BlobRequest::new(listing());
    request.first().expect("listing call");
    request.next(listed()).expect("listing answer");
    assert!(request.next(listed()).is_err());
    assert!(request.first().is_err());
}

#[test]
fn checks_read_etags() {
    assert_eq!(read_listed(None, None), Some(true));
    assert_eq!(read_listed(Some("\"a\""), Some("a")), Some(true));
    assert_eq!(read_listed(Some("a"), Some("b")), None);
    // A read without an ETag imports the file, which then records none.
    assert_eq!(read_listed(Some("a"), None), Some(false));
    let listed = object("\"a\"");
    assert_eq!(observed(&listed, false).etag, None);
    assert_eq!(observed(&listed, true), listed);
}

#[test]
fn counts_outcomes() {
    let mut checkpoint = BackendImportCheckpoint::default();
    assert_eq!(tally(&mut checkpoint, "a", Ok(Outcome::Skipped)), Ok(false));
    assert_eq!(tally(&mut checkpoint, "b", Ok(Outcome::Imported)), Ok(true));
    let failed = Err(ItemError::Item("broken".to_string()));
    assert_eq!(tally(&mut checkpoint, "c", failed), Ok(true));
    assert_eq!(tally(&mut checkpoint, "d", Ok(Outcome::Ignored)), Ok(false));
    // A file changed since its listing is skipped and read again by the next run.
    assert_eq!(
        tally(&mut checkpoint, "d", Err(ItemError::Changed)),
        Ok(false)
    );
    assert_eq!(
        (checkpoint.skipped, checkpoint.imported, checkpoint.failed),
        (2, 1, 1)
    );
    assert_eq!(checkpoint.cursor.as_deref(), Some("d"));

    // A system error ends the run before the cursor passes the object.
    let error = Err(ItemError::System("storage down".to_string()));
    assert!(tally(&mut checkpoint, "e", error).is_err());
    assert_eq!(checkpoint.cursor.as_deref(), Some("d"));
}

#[test]
fn classifies_put_errors() {
    let item = |error| matches!(put_failure(error), ItemError::Item(_));
    assert!(item(PutObjectError::QuotaExceeded { limit: 1, usage: 2 }));
    assert!(item(PutObjectError::PolicyGate(PolicyGateError::Invalid)));
    // Deleted history is reported for the file; the run goes on.
    assert!(item(PutObjectError::HistoryDeleted));
    assert!(!item(PutObjectError::StorageError(
        aruna_core::errors::StorageError::TransactionConflict
    )));
    assert!(!item(PutObjectError::ImportRemoved));
    assert!(!item(PutObjectError::PolicyGate(PolicyGateError::Drift)));
    let unavailable = PolicyGateError::Unavailable {
        policy_ids: Vec::new(),
    };
    assert!(!item(PutObjectError::PolicyGate(unavailable)));

    // A backend outage while hashing ends the run; a vanished object fails alone.
    let gone = BlobError::Unreadable("gone".to_string());
    assert!(matches!(hash_failure(gone), ItemError::Item(_)));
    let down = BlobError::ReadError("connection refused".to_string());
    assert!(matches!(hash_failure(down), ItemError::System(_)));
}

#[tokio::test]
async fn notes_listing_guarded() {
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let rule = BackendImport {
        import_id: Ulid::from_parts(4, 4),
        group_id: Ulid::from_parts(3, 3),
        backend_id: Ulid::from_parts(5, 5),
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: "in/".to_string(),
        schedule: None,
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: "{dir}{name}".to_string(),
    };
    let row_key = object_key(rule.import_id, "raw/a.csv");
    let written = ImportedObject::written(Ulid::nil(), "in/a.csv".to_string(), 4, None);
    let seen = Value::from(written.to_bytes().unwrap());
    let listed = written.first_listing(&object("e1")).unwrap();
    let stored = || async {
        let read = read_values(&context, [(IMPORTED_OBJECT_KEYSPACE, row_key.clone())]).await;
        let Ok([Some(value)]) = read else {
            panic!("the row is missing")
        };
        ImportedObject::from_bytes(&value).unwrap()
    };
    let update = encode(&listed).ok().unwrap();
    let row = write_value(
        &context,
        IMPORTED_OBJECT_KEYSPACE,
        row_key.clone(),
        seen.to_vec(),
    );
    assert!(row.await.is_ok());

    // A removed rule gets no row update.
    let note = note_listing(
        &context,
        &rule,
        row_key.clone(),
        Some(&seen),
        update.clone(),
        ListingNote::default(),
    );
    assert!(note.await.is_ok());
    assert_eq!(stored().await, written);

    let value = rule.to_bytes().unwrap();
    assert!(
        write_value(&context, BACKEND_IMPORT_KEYSPACE, rule.key(), value)
            .await
            .is_ok()
    );
    // A row another write replaced meanwhile stays as that write left it.
    let other = Value::from(listed.to_bytes().unwrap());
    let note = note_listing(
        &context,
        &rule,
        row_key.clone(),
        Some(&other),
        update.clone(),
        ListingNote::default(),
    );
    assert!(note.await.is_ok());
    assert_eq!(stored().await, written);

    // A version deleted since it was compared keeps the row and the intent as they are.
    let intent = intent_key(rule.import_id, "raw/a.csv", Ulid::nil());
    let write = write_value(&context, IMPORT_INTENT_KEYSPACE, intent.clone(), vec![1]);
    assert!(write.await.is_ok());
    let version = (
        BLOB_VERSIONS_KEYSPACE.to_string(),
        Key::from(vec![7u8]),
        Some(Value::from(vec![8u8])),
    );
    let guards = [version];
    let note = |retired| ListingNote {
        guards: guards.to_vec(),
        retired,
        ..ListingNote::default()
    };
    let held = note_listing(
        &context,
        &rule,
        row_key.clone(),
        Some(&seen),
        update.clone(),
        note(vec![intent.clone()]),
    );
    assert!(held.await.is_ok());
    assert_eq!(stored().await, written);
    let intent_left = || async {
        let read = read_values(&context, [(IMPORT_INTENT_KEYSPACE, intent.clone())]).await;
        matches!(read, Ok([Some(_)]))
    };
    assert!(intent_left().await);

    // The committed row retires the intent in the same transaction.
    let write = write_value(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        guards[0].1.clone(),
        vec![8],
    );
    assert!(write.await.is_ok());
    let retired = vec![intent.clone()];
    let done = note_listing(
        &context,
        &rule,
        row_key.clone(),
        Some(&seen),
        update,
        note(retired),
    );
    assert!(done.await.is_ok());
    assert_eq!(stored().await, listed);
    assert!(!intent_left().await);
}

#[tokio::test]
async fn retires_move_intents() {
    // A move tags with its attempt id; its intents of the version go wherever the file is.
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let (import_id, id, attempt) = (
        Ulid::from_parts(4, 4),
        Ulid::from_parts(5, 5),
        Ulid::from_parts(6, 6),
    );
    let tagged = format!("raw/a~{attempt}");
    let moved = intent_key(import_id, &tagged, id);
    let other = intent_key(import_id, &tagged, Ulid::from_parts(7, 7));
    for key in [moved.clone(), other] {
        assert!(
            write_value(&context, IMPORT_INTENT_KEYSPACE, key, vec![1])
                .await
                .is_ok()
        );
    }
    let mut expected = intent_keys(import_id, "raw/a", id, true);
    expected.push(moved);
    for path in ["raw/a", tagged.as_str()] {
        let found = stored_intents(&context, import_id, path, id).await;
        let found = found.ok().unwrap();
        assert!(expected.iter().all(|key| found.contains(key)), "{path}");
    }
    // A suffix name ends in the version id; its move tags it once more.
    let named = format!("raw/b~{id}");
    let sibling = intent_key(import_id, &format!("{named}~{attempt}"), id);
    let write = write_value(&context, IMPORT_INTENT_KEYSPACE, sibling.clone(), vec![1]);
    assert!(write.await.is_ok());
    let found = stored_intents(&context, import_id, &named, id).await;
    let found = found.ok().unwrap();
    assert!(found.contains(&sibling));
    assert!(found.contains(&intent_key(import_id, &named, id)));
}

#[test]
fn retires_both_destinations() {
    let (import_id, id) = (Ulid::from_parts(4, 4), Ulid::from_parts(5, 5));
    let tagged = format!("raw/a~{id}");
    let expected = intent_keys(import_id, "raw/a", id, true);
    // Whichever path the bytes reached, both intents of the write go.
    assert_eq!(write_intents(import_id, "raw/a", id), expected);
    assert_eq!(write_intents(import_id, &tagged, id), expected);
}

#[test]
fn keeps_alias_row() {
    let rule = recovery_rule("{dir}{id}_{name}");
    let location = BackendLocation {
        backend: BackendRef::Group(rule.backend_id),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: "raw/a.csv".to_string(),
        ulid: Ulid::from_parts(7, 7),
        format: Default::default(),
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: Default::default(),
    };
    let row = ImportedObject::new(Ulid::nil(), &object("e1"));
    let spaces = |keep_row| {
        let rows = import_rows(&rule, &location, Some("v1".to_string()), &row, keep_row);
        let rows = rows.ok().unwrap();
        rows.into_iter()
            .map(|(space, ..)| space)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        spaces(false),
        [IMPORTED_OBJECT_KEYSPACE, BACKEND_VERSION_KEYSPACE]
    );
    // An alias keeps its literal row until the alias is hidden after the commit.
    assert_eq!(spaces(true), [BACKEND_VERSION_KEYSPACE]);
}

#[tokio::test]
async fn hides_literal_alias() {
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let rule = recovery_rule("{dir}{id}_{name}");
    let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
    let spec = BackendImportSpec {
        auth_context: aruna_core::structs::identity::auth::AuthContext {
            user_id: UserId::local(Ulid::from_parts(2, 2), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
        group_id: rule.group_id,
        import_id: rule.import_id,
    };
    let (alias, other) = (Ulid::from_parts(7, 7), Ulid::from_parts(8, 8));
    let literal = "in/x_a.csv";
    let version = BlobVersion::materialized(
        [7; 32],
        BackendRef::Group(rule.backend_id),
        aruna_core::structs::storage::format::EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    for id in [alias, other] {
        let key = Key::from(VersionKey::new("bucket", literal, id).to_bytes().unwrap());
        let value = version.to_bytes().unwrap();
        let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value);
        assert!(write.await.is_ok());
    }
    let head_key = Key::from(BlobHeadKey::new("bucket", literal).to_bytes().unwrap());
    let set_head = |id| {
        let value = CurrentVersionPointer::new(id).to_bytes().unwrap();
        write_value(&context, BLOB_HEAD_KEYSPACE, head_key.clone(), value)
    };
    let head = || read_head(&context, "bucket", literal);

    // A write to the literal key meanwhile stays current.
    assert!(set_head(other).await.is_ok());
    let hidden = hide_alias(&context, &spec, &rule, literal, alias);
    assert!(hidden.await.is_ok());
    assert_eq!(head().await.ok().flatten(), Some(other));

    // The alias head gets a delete marker; its version stays for pinned references.
    assert!(set_head(alias).await.is_ok());
    let hidden = hide_alias(&context, &spec, &rule, literal, alias);
    assert!(hidden.await.is_ok());
    let marker = head().await.ok().flatten().unwrap();
    assert_ne!(marker, alias);
    let kept = exact_version(&context, "bucket", literal, alias).await;
    assert!(matches!(kept, Ok((_, Exact::Materialized(..)))));
    let (_, marker) = exact_version(&context, "bucket", literal, marker)
        .await
        .ok()
        .unwrap();
    assert_eq!(marker, Exact::Incompatible);
}

#[tokio::test]
async fn reads_key_trail() {
    use aruna_core::structs::storage::delete_audit::{BlobAuditKind, trail_key};
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let rule = recovery_rule("{dir}{name}");
    let id = Ulid::from_parts(1_000, 1);
    let deleted = || trail_of(&context, &rule, 5, "in/a.csv", id);
    assert!(!deleted().await.ok().unwrap());
    let record = |key: &str| aruna_core::structs::storage::delete_audit::BlobAuditRecord {
        realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
        group_id: rule.group_id,
        node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
        user_id: Default::default(),
        kind: BlobAuditKind::DeleteMarker,
        bucket: "bucket".to_string(),
        key: key.to_string(),
        version_id: Some(Ulid::nil()),
        occurred_at_ms: 2_000,
    };
    // A delete of another key is not read at all.
    for key in ["in/a.csv2", "in/a.csv"] {
        let record = record(key);
        let trail = Key::from(trail_key(&record, 5, Ulid::generate()));
        let value = record.to_bytes().unwrap();
        let write = write_value(&context, DELETE_TRAIL_KEYSPACE, trail, value);
        assert!(write.await.is_ok());
        assert_eq!(deleted().await.ok().unwrap(), key == "in/a.csv");
    }
    // A bucket created again under the name starts with a clean trail.
    let fresh = trail_of(&context, &rule, 6, "in/a.csv", id).await;
    assert!(!fresh.ok().unwrap());
}

#[tokio::test]
async fn classifies_exact_versions() {
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let id = Ulid::from_parts(9, 9);
    let exact = || async {
        let found = exact_version(&context, "bucket", "in/a.csv", id).await;
        found.ok().unwrap().1
    };
    assert_eq!(exact().await, Exact::Absent);
    // A deleted version holds no bytes, so a file naming it is a conflict, not new.
    let deleted = BlobVersion::deleted(std::time::SystemTime::UNIX_EPOCH, Default::default());
    let key = Key::from(
        VersionKey::new("bucket", "in/a.csv", id)
            .to_bytes()
            .unwrap(),
    );
    let write = write_value(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        key.clone(),
        deleted.to_bytes().unwrap(),
    );
    assert!(write.await.is_ok());
    assert_eq!(exact().await, Exact::Incompatible);
    let version = BlobVersion::materialized(
        [7; 32],
        BackendRef::node_default(),
        aruna_core::structs::storage::format::EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let value = version.to_bytes().unwrap();
    let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value.clone());
    assert!(write.await.is_ok());
    assert_eq!(exact().await, Exact::Materialized([7; 32], value.into()));
}

#[tokio::test]
async fn reads_committed_md5() {
    use aruna_core::structs::storage::blob::BlobLocationKey;
    use aruna_core::structs::storage::format::EncodingClass;
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let row = ImportedObject::written(Ulid::from_parts(6, 6), "in/a.csv".to_string(), 4, None);
    let listed = object("e1");
    let md5 = || committed_md5(&context, "bucket", "in/a.csv", &row, &listed);
    assert_eq!(md5().await.ok(), Some(None));

    let backend = BackendRef::Group(Ulid::from_parts(5, 5));
    let version = BlobVersion::materialized(
        [7; 32],
        backend.clone(),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let key = VersionKey::new("bucket", "in/a.csv", row.version_id);
    let key = Key::from(key.to_bytes().unwrap());
    let value = version.to_bytes().unwrap();
    let write = write_value(&context, BLOB_VERSIONS_KEYSPACE, key, value);
    assert!(write.await.is_ok());
    // A version without a location row has no MD5 to compare.
    assert_eq!(md5().await.ok(), Some(Some(None)));

    let location = BackendLocation {
        backend: backend.clone(),
        storage_class: None,
        root: String::new(),
        storage_bucket: String::new(),
        backend_path: "in/a.csv".to_string(),
        ulid: Ulid::from_parts(7, 7),
        format: Default::default(),
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: [(HASH_MD5.to_string(), vec![1; 16])].into(),
    };
    let key = BlobLocationKey::new([7; 32], EncodingClass::Raw, backend).to_bytes();
    let value = location.to_bytes().unwrap();
    let write = write_value(&context, BLOB_LOCATIONS_KEYSPACE, key.into(), value);
    assert!(write.await.is_ok());
    assert_eq!(md5().await.ok(), Some(Some(Some(vec![1; 16]))));
    // A listing without an ETag never reads the location.
    let mut unmarked = object("e1");
    unmarked.etag = None;
    let blind = committed_md5(&context, "bucket", "in/a.csv", &row, &unmarked);
    assert_eq!(blind.await.ok(), Some(Some(None)));
}

#[test]
fn compares_single_etags() {
    let md5 = [0xab; 16];
    let etag = format!("\"{}\"", hex::encode(md5));
    assert!(!etag_differs(Some(&etag), Some(&md5)));
    assert!(!etag_differs(Some(&etag.to_uppercase()), Some(&md5)));
    assert!(etag_differs(Some(&etag), Some(&[0xcd; 16])));
    // Multipart ETags, unknown MD5s and missing ETags fall back to the size.
    let multipart = format!("{}-3", hex::encode([0xcd; 16]));
    assert!(!etag_differs(Some(&multipart), Some(&md5)));
    assert!(!etag_differs(Some(&etag), None));
    assert!(!etag_differs(None, Some(&md5)));
}

fn recovery_rule(pattern: &str) -> BackendImport {
    BackendImport {
        import_id: Ulid::from_parts(4, 4),
        group_id: Ulid::from_parts(3, 3),
        backend_id: Ulid::from_parts(5, 5),
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: "in/".to_string(),
        schedule: None,
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: pattern.to_string(),
    }
}

/// The hash every test claim recorded for its written bytes.
const HASH: [u8; 32] = [7; 32];

#[tokio::test]
async fn finds_copy_candidates() {
    // Only versions a file of this size and ETag could hold are hashed against: on the
    // rule's storage, of a linked encoding, of the size and not refuted by the ETag.
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let rule = recovery_rule(aruna_core::structs::storage::backend_import::DEFAULT_PATTERN);
    let ours = BackendRef::Group(rule.backend_id);
    let other = BackendRef::Group(Ulid::from_parts(6, 6));
    let raw = EncodingClass::Raw;
    let plain = |at: u64| EncodingClass::Plain {
        location: Ulid::from_parts(at, 1),
    };
    let md5 = |byte: u8| Some(vec![byte; 16]);
    let seeds = [
        (1, ours.clone(), raw, 4, None),
        (2, other, plain(2), 4, None),
        (3, ours.clone(), raw, 9, None),
        (4, ours.clone(), plain(4), 4, md5(0)),
        (5, ours, plain(5), 4, md5(1)),
    ];
    for (at, backend, encoding, size, md5) in seeds {
        let id = Ulid::from_parts(at, 9);
        let hash = [at as u8; 32];
        let version = BlobVersion::materialized(
            hash,
            backend.clone(),
            encoding,
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let location_key = version.location_key().unwrap();
        let mut hashes = std::collections::HashMap::from([(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            hash.to_vec(),
        )]);
        if let Some(md5) = md5 {
            hashes.insert(HASH_MD5.to_string(), md5);
        }
        let location = BackendLocation {
            backend,
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: format!("raw/{at}"),
            ulid: Ulid::from_parts(at, 1),
            format: Default::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes,
        };
        let version_key = VersionKey::new("bucket", "in/a", id).to_bytes().unwrap();
        let rows = [
            (
                BLOB_VERSIONS_KEYSPACE,
                version_key,
                version.to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                location_key.to_bytes(),
                location.to_bytes().unwrap(),
            ),
        ];
        for (space, key, value) in rows {
            assert!(
                write_value(&context, space, key.into(), value)
                    .await
                    .is_ok()
            );
        }
    }
    // A long history before them is read to its end.
    let filler = BlobVersion::materialized(
        [9; 32],
        BackendRef::Group(Ulid::from_parts(6, 6)),
        EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let writes = (0..1100)
        .map(|at| {
            let key = VersionKey::new("bucket", "in/a", Ulid::from_parts(0, at));
            let value = filler.to_bytes().unwrap().into();
            let key = key.to_bytes().unwrap().into();
            (BLOB_VERSIONS_KEYSPACE.to_string(), key, value)
        })
        .collect();
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(write).await;
    let object = ExternalObject {
        key: "raw/a".to_string(),
        size: 4,
        etag: Some(hex::encode([0u8; 16])),
        modified_ms: None,
    };
    let found = candidates(&context, &rule, "in/a", &object)
        .await
        .ok()
        .unwrap();
    let ids: Vec<_> = found
        .iter()
        .map(|(_, _, id, _)| id.timestamp_ms())
        .collect();
    assert_eq!(ids, [1, 4]);
    // Without one, a run refuses the file at once and never reads it.
    let object = ExternalObject { size: 5, ..object };
    let found = candidates(&context, &rule, "in/a", &object)
        .await
        .ok()
        .unwrap();
    assert!(found.is_empty());
}

fn claim(rule: &BackendImport, key: &str, committed: bool) -> Claim {
    let intent = WriteIntent {
        key: key.to_string(),
        version_id: Ulid::generate(),
        backend: BackendRef::Group(rule.backend_id),
        created_by: Default::default(),
        written: Some(HASH),
        mover: None,
    };
    let stored = intent_key(rule.import_id, "raw/a.csv", intent.version_id);
    (stored, intent, committed)
}

#[test]
fn covers_own_write() {
    let rule = recovery_rule("{dir}{name}");
    let id = Ulid::from_parts(5, 5);
    let row = ImportedObject::written(id, "in/a.csv".to_string(), 4, None);
    let committed = |key: &str, version_id: Ulid| {
        let mut claim = claim(&rule, key, true);
        claim.1.version_id = version_id;
        claim
    };
    // The row's own committed write needs no hash.
    assert!(row_covers(Some(&row), &[committed("in/a.csv", id)]));
    // A committed move of another version onto the same name is checked by its hash.
    let moved = committed("in/a.csv", Ulid::from_parts(6, 6));
    assert!(!row_covers(Some(&row), &[moved]));
    assert!(!row_covers(Some(&row), &[committed("in/b.csv", id)]));
    assert!(!row_covers(Some(&row), &[claim(&rule, "in/a.csv", false)]));
    assert!(!row_covers(None, &[]));
}

#[test]
fn looks_up_intents() {
    let rule = recovery_rule("{dir}{id}_{name}");
    let listed = object("e1");
    assert!(needs_lookup((&rule, &[]), &listed, None, false));
    // A known file without intents takes the fast path, changed or not.
    let literal = row(Ulid::nil(), "e1");
    assert!(!needs_lookup((&rule, &[]), &listed, Some(&literal), false));
    let keyed = ImportedObject {
        key: Some("in/a.csv".to_string()),
        ..literal.clone()
    };
    assert!(!needs_lookup(
        (&rule, &[]),
        &object("e2"),
        Some(&keyed),
        false
    ));
    // An intent at the path is checked before any skip, also for an unchanged file.
    assert!(needs_lookup((&rule, &[]), &listed, Some(&keyed), true));
    assert!(needs_lookup((&rule, &[]), &listed, Some(&literal), true));
    // A literal import whose name encodes a version is checked as well.
    let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let named = ExternalObject {
        key: format!("raw/{id}_a.csv"),
        ..object("e1")
    };
    assert!(needs_lookup((&rule, &[]), &named, Some(&literal), false));
    assert!(!needs_lookup((&rule, &[]), &named, Some(&keyed), false));
    // So is one an earlier pattern of the rule named.
    let suffix = recovery_rule("{dir}{name}~{id}");
    assert!(!needs_lookup((&suffix, &[]), &named, Some(&literal), false));
    let earlier = ["{dir}{id}_{name}".to_string()];
    assert!(needs_lookup(
        (&suffix, &earlier),
        &named,
        Some(&literal),
        false
    ));
}

#[test]
fn chooses_identity() {
    let rule = recovery_rule("{dir}{id}_{name}");
    let id = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let path = format!("raw/x/{id}_a.csv");
    let decoded = choose_identity((&rule, &[], &[]), &path, Vec::new(), None)
        .ok()
        .flatten();
    // A name alone names the version, not the stripped folder `in/`: found files go to the
    // bucket root or where the discovery rules place them.
    let expected = Identity {
        key: "x/a.csv".to_string(),
        version_id: id,
        intent: None,
    };
    assert_eq!(decoded, Some(expected));
    // After a switch to suffix names the replaced pattern still reads the file.
    let suffix = recovery_rule("{dir}{name}~{id}");
    assert!(matches!(
        choose_identity((&suffix, &[], &[]), &path, Vec::new(), None),
        Ok(None)
    ));
    let earlier = ["{dir}{id}_{name}".to_string()];
    let found = choose_identity((&suffix, &earlier, &[]), &path, Vec::new(), None)
        .ok()
        .flatten();
    assert_eq!(
        found.map(|found| (found.key, found.version_id)),
        Some(("x/a.csv".to_string(), id))
    );
    let sorted = [DiscoveryRule {
        pattern: "*.csv".to_string(),
        folder: "tables/".to_string(),
    }];
    let placed = choose_identity((&rule, &[], &sorted), &path, Vec::new(), None);
    let placed = placed
        .ok()
        .flatten()
        .map(|found| (found.key, found.version_id));
    assert_eq!(placed, Some(("tables/x/a.csv".to_string(), id)));
    assert!(matches!(
        choose_identity((&rule, &[], &[]), "raw/x/a.csv", Vec::new(), None),
        Ok(None)
    ));
    // A pattern without the folder gives the name and version only.
    let stripped = recovery_rule("{id}_{name}");
    let path = format!("raw/{id}_a.csv");
    let found = choose_identity((&stripped, &[], &[]), &path, Vec::new(), None);
    let found = found
        .ok()
        .flatten()
        .map(|found| (found.key, found.version_id));
    assert_eq!(found, Some(("a.csv".to_string(), id)));
    // The intent of the write that never committed names the full key.
    let open = claim(&stripped, "in/deep/a.csv", false);
    let done = claim(&stripped, "in/other/a.csv", true);
    let chosen = choose_identity(
        (&stripped, &[], &[]),
        &path,
        vec![done.clone(), open.clone()],
        Some(&HASH),
    );
    let chosen = chosen.ok().flatten().unwrap();
    assert_eq!(
        (chosen.key.as_str(), chosen.version_id),
        ("in/deep/a.csv", open.1.version_id)
    );
    assert_eq!(chosen.intent.map(|(key, _)| key), Some(open.0.clone()));
    let only = choose_identity(
        (&stripped, &[], &[]),
        &path,
        vec![done.clone()],
        Some(&HASH),
    )
    .ok()
    .flatten();
    assert_eq!(
        only.map(|identity| identity.version_id),
        Some(done.1.version_id)
    );
    let second = claim(&stripped, "in/b.csv", false);
    assert!(
        choose_identity(
            (&stripped, &[], &[]),
            &path,
            vec![open.clone(), second],
            Some(&HASH)
        )
        .is_err()
    );
    // An intent for another backend says nothing about this file; its name still does.
    let mut foreign = open;
    foreign.1.backend = BackendRef::Group(Ulid::from_parts(9, 9));
    let found = choose_identity((&stripped, &[], &[]), &path, vec![foreign], Some(&HASH));
    let found = found
        .ok()
        .flatten()
        .map(|found| (found.key, found.intent.is_none()));
    assert_eq!(found, Some(("a.csv".to_string(), true)));
    // An open write without a record of these bytes proves nothing; it is reported.
    let mut unproven = claim(&stripped, "in/deep/a.csv", false);
    let other = [8u8; 32];
    assert!(
        choose_identity(
            (&stripped, &[], &[]),
            &path,
            vec![unproven.clone()],
            Some(&other)
        )
        .is_err()
    );
    unproven.1.written = None;
    let claims = vec![unproven, done.clone()];
    assert!(choose_identity((&stripped, &[], &[]), &path, claims, Some(&HASH)).is_err());
    // A committed write without a record of these bytes never takes over the file.
    let mut sibling = done;
    sibling.1.written = None;
    let named = recovery_rule("{dir}{name}");
    let chosen = choose_identity((&named, &[], &[]), "raw/a", vec![sibling], Some(&HASH));
    assert!(matches!(chosen, Ok(None)));
}

#[tokio::test]
async fn reads_write_claims() {
    let (_directory, storage) = crate::tests::s3::test_storage();
    let context = crate::testing::test_context(storage);
    let rule = recovery_rule("{dir}{id}_{name}");
    let (open, done) = (
        claim(&rule, "in/a.csv", false),
        claim(&rule, "in/a.csv", true),
    );
    // Another path that only starts like this one holds no claim on it.
    let longer = intent_key(rule.import_id, "raw/a.csv2", Ulid::generate());
    for (key, intent) in [(&open.0, &open.1), (&done.0, &done.1), (&longer, &open.1)] {
        let value = intent.to_bytes().unwrap();
        let write = write_value(&context, IMPORT_INTENT_KEYSPACE, key.clone(), value);
        assert!(write.await.is_ok());
    }
    let version = BlobVersion::materialized(
        [7; 32],
        BackendRef::Group(rule.backend_id),
        aruna_core::structs::storage::format::EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let key = VersionKey::new("bucket", "in/a.csv", done.1.version_id);
    let key = Key::from(key.to_bytes().unwrap());
    let write = write_value(
        &context,
        BLOB_VERSIONS_KEYSPACE,
        key,
        version.to_bytes().unwrap(),
    );
    assert!(write.await.is_ok());

    // One range read covers the page; a path past its last object is not read.
    let far = intent_key(rule.import_id, "raw/z.csv", Ulid::generate());
    let value = open.1.to_bytes().unwrap();
    assert!(
        write_value(&context, IMPORT_INTENT_KEYSPACE, far, value)
            .await
            .is_ok()
    );
    let page = [
        object("e1"),
        ExternalObject {
            key: "raw/b.csv".to_string(),
            ..object("e1")
        },
    ];
    let intents = page_intents(&context, &rule, &page).await.ok().unwrap();
    let paths: Vec<&str> = intents.keys().map(String::as_str).collect();
    assert_eq!(paths, ["raw/a.csv", "raw/a.csv2"]);
    let mut claims = read_claims(&context, &rule, &intents["raw/a.csv"])
        .await
        .ok()
        .unwrap();
    claims.sort_by_key(|(_, intent, _)| intent.version_id);
    let mut expected = vec![open.clone(), done.clone()];
    expected.sort_by_key(|(_, intent, _)| intent.version_id);
    assert_eq!(claims, expected);
}

#[test]
fn keeps_recorded_keys() {
    let mut rule = rule_at("");
    rule.write_pattern = "{name}".to_string();
    let discovery = [DiscoveryRule {
        pattern: "*.txt".to_string(),
        folder: "texts/".to_string(),
    }];
    let key = |row: Option<&ImportedObject>, path: &str| {
        bucket_key((&rule, &[], &discovery), row, path).ok()
    };
    // Uploads of `foo/bar.txt` and `bar/foo.txt` flattened into one folder keep their keys.
    let bar = ImportedObject::written(Ulid::generate(), "foo/bar.txt".to_string(), 4, None);
    let foo = ImportedObject::written(Ulid::generate(), "bar/foo.txt".to_string(), 4, None);
    assert_eq!(
        key(Some(&bar), "raw/bar.txt").as_deref(),
        Some("foo/bar.txt")
    );
    assert_eq!(
        key(Some(&foo), "raw/foo.txt").as_deref(),
        Some("bar/foo.txt")
    );
    // An older row without a key keeps the rule's own mapping, whatever the patterns say.
    let older = row(Ulid::generate(), "e1");
    assert_eq!(key(Some(&older), "raw/a.txt").as_deref(), Some("a.txt"));
    // A new file goes to the root unless one pattern places it.
    assert_eq!(key(None, "raw/baz").as_deref(), Some("baz"));
    assert_eq!(
        key(None, "raw/x/new.txt").as_deref(),
        Some("texts/x/new.txt")
    );
    let overlapping = [
        discovery[0].clone(),
        DiscoveryRule {
            pattern: "new.*".to_string(),
            folder: "other/".to_string(),
        },
    ];
    let clash = bucket_key((&rule, &[], &overlapping), None, "raw/new.txt");
    assert!(matches!(clash, Err(ItemError::Item(_))));
    // The rule's own folder is where its uploads come from, not where found files go.
    let foo = rule_at("foo/");
    let found = |path| bucket_key((&foo, &[], &discovery), None, path).ok();
    assert_eq!(found("raw/baz").as_deref(), Some("baz"));
    assert_eq!(found("raw/x/new.txt").as_deref(), Some("texts/x/new.txt"));
}

fn rule_at(key_prefix: &str) -> BackendImport {
    BackendImport {
        import_id: Ulid::nil(),
        group_id: Ulid::nil(),
        backend_id: Ulid::nil(),
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: key_prefix.to_string(),
        schedule: None,
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: "{dir}{name}".to_string(),
    }
}

#[test]
fn imports_new_objects() {
    assert_eq!(decide(&object("e1"), None, None, false), Decision::Import);
    // A key the bucket already holds is a conflict, never taken over.
    let other = Some(Ulid::generate());
    assert_eq!(decide(&object("e1"), None, other, false), Decision::Taken);
}

#[test]
fn skips_unchanged_objects() {
    let version = Ulid::generate();
    let row = row(version, "e1");
    assert_eq!(
        decide(&object("e1"), Some(&row), Some(version), true),
        Decision::Skip
    );
    // A purged version is imported again.
    assert_eq!(
        decide(&object("e1"), Some(&row), None, false),
        Decision::Import
    );
    assert_eq!(
        decide(&object("e1"), Some(&row), None, true),
        Decision::Skip
    );
}

#[test]
fn aruna_side_wins() {
    let version = Ulid::generate();
    let row = row(version, "e1");
    let changed = object("e2");
    let fresh = decide(&changed, Some(&row), Some(version), true);
    assert_eq!(fresh, Decision::Import);
    assert_eq!(decide(&changed, Some(&row), None, true), Decision::Import);
    // A later write or delete marker in Aruna keeps the key as Aruna has it.
    let newer = Some(Ulid::generate());
    assert_eq!(decide(&changed, Some(&row), newer, true), Decision::Skip);
    assert_eq!(
        decide(&object("e1"), Some(&row), newer, true),
        Decision::Skip
    );
}

#[test]
fn skips_known_pages() {
    let rule = rule_at("");
    let id = Ulid::generate();
    let key = rule.aruna_key("raw/a.csv").unwrap();
    let version = BlobVersion::materialized(
        [1u8; 32],
        BackendRef::Group(Ulid::nil()),
        aruna_core::structs::storage::format::EncodingClass::Raw,
        std::time::SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    );
    let mut page = PageReads::default();
    let stored = row(id, "e1").to_bytes().unwrap().into();
    page.rows.insert("raw/a.csv".to_string(), Some(stored));
    page.heads.insert(key, Some(id));
    let version = version.to_bytes().unwrap().into();
    page.versions.insert("raw/a.csv".to_string(), Some(version));
    assert!(page_skip((&rule, &[]), &page, false, &object("e1")));
    // A changed file, an open intent or an unread page takes the full path.
    assert!(!page_skip((&rule, &[]), &page, false, &object("e2")));
    assert!(!page_skip((&rule, &[]), &page, true, &object("e1")));
    assert!(!page_skip(
        (&rule, &[]),
        &PageReads::default(),
        false,
        &object("e1")
    ));
    // Without ETag and time an equal size skips only when the row would stay the same.
    let mut unmarked = object("e1");
    unmarked.etag = None;
    assert!(page_skip((&rule, &[]), &page, false, &unmarked));
    page.versions.insert("raw/a.csv".to_string(), None);
    assert!(!page_skip((&rule, &[]), &page, false, &unmarked));
}

#[test]
fn trusts_equal_sizes() {
    // Aruna's own write is not read again; its first listing only adds ETag and time.
    let written = ImportedObject::written(Ulid::generate(), "in/a.csv".to_string(), 4, None);
    let listed = trusted_listing(Some(&written), &object("e1")).unwrap();
    assert_eq!(listed.etag.as_deref(), Some("e1"));
    let mut grown = object("e1");
    grown.size = 5;
    assert_eq!(trusted_listing(Some(&written), &grown), None);
    // An imported row is compared by ETag later, never trusted here.
    let imported = row(Ulid::generate(), "e1");
    assert_eq!(trusted_listing(Some(&imported), &object("e2")), None);
    let mut unmarked = object("e1");
    (unmarked.etag, unmarked.modified_ms) = (None, None);
    let blind = trusted_listing(Some(&imported), &unmarked);
    assert_eq!(blind, Some(imported.clone()));
    unmarked.size = 5;
    assert_eq!(trusted_listing(Some(&imported), &unmarked), None);
    assert_eq!(trusted_listing(None, &unmarked), None);
}

/// A job of the test's spec.
fn job_of(spec: &BackendImportSpec) -> ImportJob {
    ImportJob {
        spec: Arc::new(spec.clone()),
        job_id: JobId::from_bytes([1; 16]),
        claim_token: Ulid::from_bytes([2; 16]),
        routing: Default::default(),
        seed: 3,
    }
}

async fn write_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    value: Vec<u8>,
) -> Result<(), ItemError> {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key,
        value: value.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(ItemError::System(format!("unexpected answer {other:?}"))),
    }
}

async fn read_values<const N: usize>(
    context: &DriverContext,
    reads: [(&str, Key); N],
) -> Result<[Option<Value>; N], ItemError> {
    let reads = reads
        .into_iter()
        .map(|(key_space, key)| (key_space.to_string(), key))
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            let values: Vec<_> = values.into_iter().map(|(_, value)| value).collect();
            values
                .try_into()
                .map_err(|_| ItemError::System("short batch read".to_string()))
        }
        other => Err(ItemError::System(format!("unexpected answer {other:?}"))),
    }
}

async fn read_head(
    context: &DriverContext,
    bucket: &str,
    key: &str,
) -> Result<Option<Ulid>, ItemError> {
    let head_key = BlobHeadKey::new(bucket, key).to_bytes().unwrap();
    let [head] = read_values(context, [(BLOB_HEAD_KEYSPACE, head_key.into())]).await?;
    Ok(parse(head, CurrentVersionPointer::from_bytes)?.map(|head| head.version_id))
}

async fn exact_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> Result<(Key, Exact), ItemError> {
    let row = version_row(bucket, key, version_id)?;
    let [value] = read_values(context, [(BLOB_VERSIONS_KEYSPACE, row.clone())]).await?;
    Ok((row, exact_of(value)?))
}

async fn stored_intents(
    context: &DriverContext,
    import_id: Ulid,
    path: &str,
    id: Ulid,
) -> Result<Vec<Key>, ItemError> {
    drive(StoredIntentsOperation::new(import_id, path, id), context).await
}

async fn hide_alias(
    context: &DriverContext,
    spec: &BackendImportSpec,
    rule: &BackendImport,
    literal: &str,
    version_id: Ulid,
) -> Result<(), ItemError> {
    let (job, rule, literal) = (job_of(spec), rule.clone(), literal.to_string());
    drive(
        HideAliasOperation::new(job, rule, literal, version_id),
        context,
    )
    .await
}

async fn trail_of(
    context: &DriverContext,
    rule: &BackendImport,
    incarnation: u128,
    key: &str,
    id: Ulid,
) -> Result<bool, ItemError> {
    drive(TrailOperation::new(rule, incarnation, key, id), context).await
}

async fn committed_md5(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    row: &ImportedObject,
    object: &ExternalObject,
) -> Result<Option<Option<Vec<u8>>>, ItemError> {
    drive(
        CommittedMd5Operation::new(bucket, key, row, object)?,
        context,
    )
    .await
}

async fn candidates(
    context: &DriverContext,
    rule: &BackendImport,
    key: &str,
    object: &ExternalObject,
) -> Result<Vec<reads::Candidate>, ItemError> {
    drive(CandidatesOperation::new(rule, key, object)?, context).await
}

async fn read_claims(
    context: &DriverContext,
    rule: &BackendImport,
    intents: &[(Key, WriteIntent)],
) -> Result<Vec<Claim>, ItemError> {
    drive(ClaimsOperation::new(rule, intents.to_vec()), context).await
}

async fn page_intents(
    context: &DriverContext,
    rule: &BackendImport,
    objects: &[ExternalObject],
) -> Result<PathIntents, ItemError> {
    drive(PageIntentsOperation::new(rule.import_id, objects), context)
        .await
        .map_err(|error| ItemError::System(error.0))
}

async fn note_listing(
    context: &DriverContext,
    rule: &BackendImport,
    row_key: Key,
    seen: Option<&Value>,
    row: Vec<u8>,
    note: ListingNote,
) -> Result<bool, ItemError> {
    let operation =
        NoteListingOperation::new(rule.clone(), row_key, seen.cloned(), row.into(), note);
    match drive(operation, context).await {
        Ok(written) => Ok(written),
        Err(NoteError::Io(IoError::Conflict)) => Ok(false),
        Err(error) => Err(ItemError::System(error.to_string())),
    }
}
