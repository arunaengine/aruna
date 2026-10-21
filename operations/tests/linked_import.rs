//! A linked storage on a real S3 endpoint: discovery previews and the import job end to end.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

use aruna_blob::blob::{BackendRegistry, BlobHandler, NodeBackend};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord, JobResultPayload};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportSpec, DEFAULT_PATTERN, PlainPath,
};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendRef, BlobHeadKey, BucketInfo, CurrentVersionPointer,
    ResolvedBackend,
};
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::discovery::DiscoveryRule;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::group_backend::{
    GroupBackendKind, GroupStorage, GroupStorageSecret,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::auth::request_authorization::BucketAccess;
use aruna_operations::blob::unify::{Unified, preview_page, unify};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::groups::backends::record_writes;
use aruna_operations::groups::fence::GroupWriteOperation;
use aruna_operations::groups::imports::create::{CreateImportInput, CreateImportOperation};
use aruna_operations::groups::imports::discovery::{
    FileOutcome, SetDiscoveryOperation, authorized_preview,
};
use aruna_operations::jobs::backend_import::run_backend_import;
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::store::{ClaimOutcome, claim_job, insert_job, transition_to_running};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_storage::storage;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const BUCKET: &str = "bucket";

struct TestContext {
    _temp_dir: TempDir,
    driver: Arc<DriverContext>,
    auth: AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    rule: BackendImport,
    /// The provider bucket of the rule's folder.
    folder: String,
}

fn variable(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

async fn write_value(driver: &DriverContext, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
    let write = StorageEffect::Write {
        key_space: keyspace.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let event = driver.storage_handle.send_storage_effect(write).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// The realm, the group with its owner and the bucket, as an import job needs them.
async fn seed_group(driver: &DriverContext, actor: &Actor, group_id: GroupId) {
    let (realm_id, user_id) = (actor.realm_id, actor.user_id);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::default_group_doc(user_id, realm_id, group_id);
    let group = Group {
        display_name: "linked".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: user_id,
    };
    let realm_key = realm_id.as_bytes().to_vec();
    let group_key = group_id.to_bytes().to_vec();
    let rows = [
        (
            REALM_CONFIG_KEYSPACE,
            realm_key.clone(),
            config.to_bytes(actor),
        ),
        (AUTH_KEYSPACE, realm_key, realm_auth.to_bytes(actor)),
        (AUTH_KEYSPACE, group_key.clone(), group_auth.to_bytes(actor)),
        (GROUP_KEYSPACE, group_key, group.to_bytes(actor)),
    ];
    for (keyspace, key, value) in rows {
        write_value(driver, keyspace, key, value.unwrap()).await;
    }
    let info = BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let create = CreateBucketOperation::new(BUCKET.to_string(), info);
    drive(create, driver).await.unwrap();
}

/// A node whose own data stays on its filesystem, with a group storage on the S3 endpoint
/// and a rule that links the folder `raw/` of a new provider bucket to the bucket root.
async fn setup_context() -> TestContext {
    setup_pattern(DEFAULT_PATTERN).await
}

/// The same with the rule's write pattern `pattern`.
async fn setup_pattern(pattern: &str) -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let root = temp_dir.path().to_str().unwrap().to_string();
    let storage_handle = storage::FjallStorage::open(&root).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    std::fs::create_dir_all(format!("{root}/blobs")).unwrap();
    let node = NodeBackend::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: format!("{root}/blobs"),
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-".to_string()),
            max_bucket_size: None,
            multipart_bucket: Some("aruna-parts".to_string()),
            timeouts: Default::default(),
        },
        None,
    );
    let mut backends = BTreeMap::new();
    backends.insert(BackendRef::DEFAULT_NODE_NAME.to_string(), Arc::new(node));
    let registry = BackendRegistry::new(backends, BackendRef::DEFAULT_NODE_NAME.into()).unwrap();
    let blob_handle = BlobHandler::with_registry(
        registry,
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();
    let driver = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    seed_group(&driver, &actor, group_id).await;

    // Without a bucket the storage takes each rule's bucket from the rule's folder.
    let public_config = HashMap::from([
        ("endpoint".to_string(), variable("ARUNA_TEST_S3_ENDPOINT")),
        (
            "region".to_string(),
            std::env::var("ARUNA_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        ),
        ("force_path_style".to_string(), "true".to_string()),
    ]);
    let secret_config = HashMap::from([
        (
            "access_key_id".to_string(),
            variable("ARUNA_TEST_S3_ACCESS_KEY"),
        ),
        (
            "secret_access_key".to_string(),
            variable("ARUNA_TEST_S3_SECRET_KEY"),
        ),
    ]);
    // Stored as creation stores it; creation itself refuses a plain http endpoint.
    let backend = GroupStorage {
        backend_id: Ulid::generate(),
        group_id,
        name: "tenant".to_string(),
        kind: GroupBackendKind::S3,
        public_config,
        created_at: SystemTime::now(),
        updated_at: SystemTime::now(),
        created_by: user_id,
        disabled: false,
        cleanup: CleanupStrategy::Retain,
    };
    let secret = GroupStorageSecret {
        backend_id: backend.backend_id,
        secret_config,
        updated_at: SystemTime::now(),
    };
    let write = GroupWriteOperation::new(group_id, record_writes(&backend).unwrap()).with_secret(
        GroupStorageSecret::vault_entry(backend.backend_id),
        secret.to_secret().unwrap(),
    );
    drive(write, &driver).await.unwrap();
    let folder = format!("li-{}", &Ulid::generate().to_string().to_lowercase()[14..]);
    let rule = CreateImportOperation::new(CreateImportInput {
        group_id,
        created_by: user_id,
        backend_id: backend.backend_id,
        backend_prefix: format!("{folder}/raw/"),
        bucket: BUCKET.to_string(),
        key_prefix: String::new(),
        schedule: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: pattern.to_string(),
        now: SystemTime::now(),
        move_existing: false,
    });
    let rule = drive(rule, &driver).await.unwrap();
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    TestContext {
        _temp_dir: temp_dir,
        driver,
        auth,
        node_id,
        group_id,
        rule,
        folder,
    }
}

/// Writes a file into the rule's folder the way any other client of the storage would.
async fn place(context: &TestContext, name: &str, bytes: &[u8]) {
    let plain = PlainPath {
        path: format!("{}/raw/{name}", context.folder),
        ulid: Ulid::generate(),
        if_not_exists: false,
        exact: false,
    };
    let body = BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )));
    let effect = BlobEffect::WritePlain {
        resolved: ResolvedBackend::new(BackendRef::Group(context.rule.backend_id), None),
        plain,
        created_by: context.auth.user_id,
        blob: body,
    };
    let blob_handle = context.driver.blob_handle.as_ref().unwrap();
    let event = blob_handle.send_blob_effect(effect).await;
    assert!(
        matches!(event, Event::Blob(BlobEvent::PlainWritten { .. })),
        "{event:?}"
    );
}

fn proteins() -> Vec<DiscoveryRule> {
    vec![DiscoveryRule {
        pattern: "*.faa.gz".to_string(),
        folder: "proteins/".to_string(),
    }]
}

async fn run_import(context: &TestContext, tag: u8) -> (u64, u64, u64) {
    let spec = BackendImportSpec {
        auth_context: context.auth.clone(),
        node_id: context.node_id,
        group_id: context.group_id,
        import_id: context.rule.import_id,
    };
    let job_id = JobId::from_bytes([tag; 16]);
    let storage = &context.driver.storage_handle;
    let record = JobRecord::new(
        job_id,
        JobPayload::BackendImport(spec.clone()),
        spec.auth_context.user_id,
        spec.node_id,
        1,
        1,
        None,
    );
    insert_job(storage, &record).await.unwrap();
    let now = unix_timestamp_millis();
    let ClaimOutcome::Claimed(claimed) =
        claim_job(storage, job_id, spec.node_id, now).await.unwrap()
    else {
        panic!("the import job was not claimed")
    };
    let claim_token = claimed.claim.as_ref().unwrap().claim_token;
    let running = transition_to_running(storage, job_id, claim_token, now)
        .await
        .unwrap();
    let job = JobContext {
        driver: context.driver.clone(),
        job_id,
        owner_node_id: spec.node_id,
        claim_token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&running.progress),
    };
    match run_backend_import(&job, &spec).await {
        JobRunOutcome::Succeeded(JobResultPayload::BackendImport {
            imported,
            skipped,
            failed,
        }) => (imported, skipped, failed),
        JobRunOutcome::Failed(error) => panic!("the import failed: {}", error.message),
        _ => panic!("the import did not succeed"),
    }
}

async fn has_key(context: &TestContext, key: &str) -> bool {
    let read = StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: BlobHeadKey::new(BUCKET, key).to_bytes().unwrap().into(),
        txn_id: None,
    };
    let event = context
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await;
    matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    )
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_previews_discovery() {
    // A preview places each listed file by the rules and pages after a given file.
    let context = setup_context().await;
    for name in ["a.faa.gz", "b.txt", "sub/c.faa.gz"] {
        place(&context, name, name.as_bytes()).await;
    }
    let caller = (&context.auth, context.node_id);
    let preview = authorized_preview(&context.driver, caller, &context.rule, &proteins(), None)
        .await
        .unwrap();
    let placed: Vec<_> = (preview.files.iter())
        .map(|file| (file.key.as_deref(), file.outcome.clone()))
        .collect();
    assert_eq!(
        placed,
        [
            (Some("proteins/a.faa.gz"), FileOutcome::Matched),
            (Some("b.txt"), FileOutcome::Root),
            (Some("proteins/sub/c.faa.gz"), FileOutcome::Matched),
        ]
    );
    assert_eq!(preview.next, None);
    let after = Some(format!("{}/raw/b.txt", context.folder));
    let rest = authorized_preview(&context.driver, caller, &context.rule, &proteins(), after)
        .await
        .unwrap();
    assert_eq!(rest.files.len(), 1);
    assert_eq!(rest.files[0].key.as_deref(), Some("proteins/sub/c.faa.gz"));
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_imports_folder() {
    // An import run registers every file at the key the rules give it; a second run skips
    // the files it already knows.
    let context = setup_context().await;
    for name in ["a.faa.gz", "b.txt"] {
        place(&context, name, name.as_bytes()).await;
    }
    let rules = SetDiscoveryOperation::new(context.group_id, context.rule.import_id, proteins());
    drive(rules, &context.driver).await.unwrap();
    assert_eq!(run_import(&context, 0xC3).await, (2, 0, 0));
    assert!(has_key(&context, "proteins/a.faa.gz").await);
    assert!(has_key(&context, "b.txt").await);
    assert!(!has_key(&context, "a.faa.gz").await);
    // A name the write pattern made keeps the version id it encodes.
    let id = Ulid::generate();
    place(&context, &format!("c.txt~{id}"), b"later").await;
    assert_eq!(run_import(&context, 0xD4).await, (1, 2, 0));
    assert_eq!(current(&context, "c.txt").await, Some(id));
}

async fn current(context: &TestContext, key: &str) -> Option<Ulid> {
    let read = StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: BlobHeadKey::new(BUCKET, key).to_bytes().unwrap().into(),
        txn_id: None,
    };
    let event = context
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("head read failed: {event:?}")
    };
    value.map(|value| {
        CurrentVersionPointer::from_bytes(&value)
            .unwrap()
            .version_id
    })
}

#[tokio::test]
#[ignore = "requires a real S3 endpoint (ARUNA_TEST_S3_* variables)"]
async fn s3_finds_copies() {
    // A plain file with the bytes of a recovered upload of its key is that version's second
    // copy, which the duplicates preview lists; other bytes stay a conflict.
    let context = setup_pattern("{dir}{id}_{name}").await;
    let (id, other) = (Ulid::generate(), Ulid::generate());
    place(&context, &format!("old/{id}_sample.faa.gz"), b"same bytes").await;
    place(&context, "old/sample.faa.gz", b"same bytes").await;
    place(&context, &format!("old/{other}_x.txt"), b"first").await;
    place(&context, "old/x.txt", b"second").await;
    assert_eq!(run_import(&context, 0xC3).await, (3, 0, 1));
    assert_eq!(current(&context, "old/sample.faa.gz").await, Some(id));
    assert_eq!(current(&context, "old/x.txt").await, Some(other));
    // A second run knows both copies.
    assert_eq!(run_import(&context, 0xD4).await, (0, 3, 1));

    let scope = (context.auth.realm_id, context.group_id, context.node_id);
    let access = BucketAccess::load(&context.driver, &context.auth, scope, BUCKET)
        .await
        .unwrap();
    let page = preview_page(&context.driver, (BUCKET, ""), &access, (None, None, 100))
        .await
        .unwrap();
    assert_eq!((page.keys, page.bytes), (1, 10));
    let [found] = page.entries.as_slice() else {
        panic!("expected one key with copies: {page:?}")
    };
    assert_eq!(found.key, "old/sample.faa.gz");
    assert!(
        found
            .keep
            .backend_path
            .ends_with(&format!("old/{id}_sample.faa.gz"))
    );
    let [copy] = found.copies.as_slice() else {
        panic!("expected one copy: {found:?}")
    };
    assert!(copy.backend_path.ends_with("raw/old/sample.faa.gz"));
    // The cleanup lets the key keep one file, and the preview has nothing left to tidy.
    let bucket = (BUCKET, context.group_id);
    let unified = unify(&context.driver, bucket, found, None).await.unwrap();
    assert_eq!(unified, Unified::Done);
    let page = preview_page(&context.driver, (BUCKET, ""), &access, (None, None, 100))
        .await
        .unwrap();
    assert_eq!((page.keys, page.entries.len()), (0, 0));
}
