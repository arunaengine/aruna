// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use aruna_blob::blob::BlobHandler;
use aruna_core::effects::StorageEffect;
use aruna_core::egress::EgressPolicy;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::FIRST_GRANTABLE_HANDLE;
use aruna_core::structs::{
    Actor, AuthContext, Backend, BackendConfig, Group, GroupAuthorizationDocument, HarvestJobSpec,
    HarvestProvenance, HarvestRecordState, HarvestSelector, HarvestSource, JobError, JobErrorKind,
    JobProgress, JobResultPayload, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    RealmNodeKind, RepositoryConnector, RepositoryConnectorKind,
};
use aruna_core::structured_id::{BucketId, MetaResourceId, PlacementHandle};
use aruna_core::types::{GroupId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::harvest::repository::{
    connector_writes, parse_provenance_read, parse_source_read, read_provenance_effect,
    read_source_effect, write_source_effect,
};
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::harvest::run_harvest_job;
use aruna_operations::jobs::submit::mint_job_id;
use aruna_operations::metadata::forward::delete_metadata_document_routed;
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::metadata::{MetadataAuthToken, MetadataHandle};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use axum::Router;
use axum::extract::Query;
use axum::routing::get;
use byteview::ByteView;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const NAMESPACE: &str = "zenodo";
const PREFIX: &str = "imported/zenodo";
const ALPHA: &str = "oai:ex:alpha";
const BETA: &str = "oai:ex:beta";

type BoxError = Box<dyn std::error::Error>;

// ---------------------------------------------------------------- fixture

struct Fixture {
    _root: tempfile::TempDir,
    actor: Actor,
    group_id: GroupId,
    context: Arc<DriverContext>,
    net: NetHandle,
}

impl Fixture {
    async fn stop(self) {
        self.net.shutdown().await;
    }

    fn job_context(&self) -> (JobContext, CancellationToken, CancellationToken) {
        let cancel = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let ctx = JobContext {
            driver: self.context.clone(),
            job_id: mint_job_id(
                PlacementHandle::new(FIRST_GRANTABLE_HANDLE).expect("handle"),
                BucketId::new(0).expect("bucket"),
            )
            .expect("job id"),
            owner_node_id: self.actor.node_id,
            claim_token: Ulid::generate(),
            final_attempt: false,
            cancel: cancel.clone(),
            shutdown: shutdown.clone(),
            progress: ProgressReporter::from_progress(&JobProgress::new("records")),
        };
        (ctx, cancel, shutdown)
    }
}

async fn build_fixture() -> Result<Fixture, BoxError> {
    let root = tempfile::tempdir()?;
    let storage_path = root.path().join("storage");
    let metadata_path = root.path().join("metadata");
    let blob_path = root.path().join("blob");
    std::fs::create_dir_all(&storage_path)?;
    std::fs::create_dir_all(&metadata_path)?;
    std::fs::create_dir_all(&blob_path)?;
    let storage = FjallStorage::open(storage_path.to_str().ok_or("invalid storage path")?)?;
    let realm_id = RealmId::from_bytes([41; 32]);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    seed_config(&storage, &actor).await?;
    let group_id = Ulid::generate();
    seed_auth(&storage, &actor, group_id).await?;
    let blob = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_path.to_str().ok_or("invalid blob path")?.to_string(),
            service_config: Default::default(),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
        EgressPolicy::loopback(),
    )
    .await?;
    let metadata = MetadataHandle::new(
        &metadata_path,
        actor.node_id,
        storage.clone(),
        None,
        None,
        None,
    )?;
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle: Some(metadata),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    Ok(Fixture {
        _root: root,
        actor,
        group_id,
        context,
        net,
    })
}

async fn seed_config(storage: &StorageHandle, actor: &Actor) -> Result<(), BoxError> {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    write_value(
        storage,
        REALM_CONFIG_KEYSPACE,
        actor.realm_id.as_bytes().to_vec(),
        config.to_bytes(actor)?,
    )
    .await
}

async fn seed_auth(
    storage: &StorageHandle,
    actor: &Actor,
    group_id: GroupId,
) -> Result<(), BoxError> {
    let realm = RealmAuthorizationDocument::new_default_realm_doc(actor.realm_id);
    let group =
        GroupAuthorizationDocument::new_default_group_doc(actor.user_id, actor.realm_id, group_id);
    write_value(
        storage,
        AUTH_KEYSPACE,
        actor.realm_id.as_bytes().to_vec(),
        realm.to_bytes(actor)?,
    )
    .await?;
    write_value(
        storage,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(actor)?,
    )
    .await?;
    let group_doc = Group {
        display_name: "harvest".to_string(),
        group_id,
        realm_id: actor.realm_id,
        roles: HashSet::new(),
        owner: actor.user_id,
    };
    write_value(
        storage,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_doc.to_bytes(actor)?,
    )
    .await
}

async fn read_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, BoxError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.as_ref().to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(format!("unexpected storage read event: {event:?}").into()),
    }
}

async fn write_value(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), BoxError> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(format!("unexpected storage write event: {event:?}").into()),
    }
}

// ------------------------------------------------------------ OAI provider

/// Scripted OAI-PMH endpoint. Responses are keyed by the resumption token the
/// request carries (`start` for a fresh listing), so a test rewrites the script
/// between runs to model an upstream that changed.
#[derive(Clone)]
struct Provider {
    script: Arc<Mutex<HashMap<String, String>>>,
    requests: Arc<Mutex<Vec<String>>>,
    interrupt_on: Arc<Mutex<Option<(String, CancellationToken)>>>,
    endpoint: String,
}

impl Provider {
    fn script(&self, key: &str, body: impl Into<String>) {
        self.script
            .lock()
            .expect("script lock")
            .insert(key.to_string(), body.into());
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests lock").clone()
    }

    fn interrupt(&self, key: &str, token: CancellationToken) {
        *self.interrupt_on.lock().expect("interrupt lock") = Some((key.to_string(), token));
    }
}

async fn serve_oai() -> Result<(Provider, tokio::task::JoinHandle<()>), BoxError> {
    let script: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let interrupt_on: Arc<Mutex<Option<(String, CancellationToken)>>> = Arc::new(Mutex::new(None));

    let handler_script = script.clone();
    let handler_requests = requests.clone();
    let handler_interrupt = interrupt_on.clone();
    let router = Router::new().route(
        "/oai",
        get(move |Query(params): Query<HashMap<String, String>>| {
            let script = handler_script.clone();
            let requests = handler_requests.clone();
            let interrupt = handler_interrupt.clone();
            async move {
                let key = request_key(&params);
                requests.lock().expect("requests lock").push(key.clone());
                if let Some((target, token)) = interrupt.lock().expect("interrupt lock").as_ref()
                    && target == &key
                {
                    token.cancel();
                }
                script
                    .lock()
                    .expect("script lock")
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| error_page("badArgument", "unscripted request"))
            }
        }),
    );
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok((
        Provider {
            script,
            requests,
            interrupt_on,
            endpoint: format!("http://{address}/oai"),
        },
        server,
    ))
}

fn request_key(params: &HashMap<String, String>) -> String {
    if params.get("verb").map(String::as_str) == Some("Identify") {
        return "identify".to_string();
    }
    params
        .get("resumptionToken")
        .cloned()
        .unwrap_or_else(|| "start".to_string())
}

fn identify_page() -> String {
    r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><Identify>
      <repositoryName>Fixture</repositoryName>
      <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
    </Identify></OAI-PMH>"#
        .to_string()
}

fn error_page(code: &str, message: &str) -> String {
    format!(
        r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="{code}">{message}</error></OAI-PMH>"#
    )
}

fn live_record(identifier: &str, datestamp: &str, title: &str) -> String {
    format!(
        r#"<record><header>
        <identifier>{identifier}</identifier><datestamp>{datestamp}</datestamp>
      </header><metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                   xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:title><![CDATA[{title}]]></dc:title>
          <dc:creator>Alice</dc:creator>
        </oai_dc:dc>
      </metadata></record>"#
    )
}

fn deleted_record(identifier: &str, datestamp: &str) -> String {
    format!(
        r#"<record><header status="deleted">
        <identifier>{identifier}</identifier><datestamp>{datestamp}</datestamp>
      </header></record>"#
    )
}

fn list_page(records: &[String], token: Option<&str>) -> String {
    let body = records.concat();
    let token = token
        .map(|token| format!("<resumptionToken>{token}</resumptionToken>"))
        .unwrap_or_default();
    format!(
        r#"<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>{body}{token}</ListRecords></OAI-PMH>"#
    )
}

// ------------------------------------------------------------- seed + read

async fn seed_source(fixture: &Fixture, endpoint: &str) -> Result<HarvestSource, BoxError> {
    let connector = RepositoryConnector::new(
        Ulid::generate(),
        fixture.group_id,
        "fixture".to_string(),
        RepositoryConnectorKind::OaiPmh,
        endpoint.to_string(),
        HashMap::new(),
        SystemTime::now(),
        SystemTime::now(),
        fixture.actor.user_id,
    );
    for (key_space, key, value) in connector_writes(&connector, None)? {
        write_value(
            &fixture.context.storage_handle,
            &key_space,
            key.as_ref().to_vec(),
            value.as_ref().to_vec(),
        )
        .await?;
    }
    let source = HarvestSource::new(
        Ulid::generate(),
        fixture.group_id,
        connector.connector_id,
        NAMESPACE.to_string(),
        PREFIX.to_string(),
        HarvestSelector::default(),
        None,
        SystemTime::now(),
        fixture.actor.user_id,
    );
    expect_write(
        fixture
            .context
            .storage_handle
            .send_effect(write_source_effect(&source, None)?)
            .await,
    )?;
    Ok(source)
}

/// Drive the projector and materializer the way a running node's background
/// loops would, so a later harvest sees the documents an earlier one created.
async fn drain(fixture: &Fixture) -> Result<(), BoxError> {
    for _ in 0..8 {
        replay_metadata_event_log(fixture.context.as_ref())
            .await
            .map_err(|error| format!("{error:?}"))?;
        process_metadata_materialization_batch(fixture.context.as_ref())
            .await
            .map_err(|error| format!("{error:?}"))?;
    }
    Ok(())
}

/// Project the registry without materializing any graph, so a run sees a
/// durable record whose graph is not readable yet.
async fn project_only(fixture: &Fixture) -> Result<(), BoxError> {
    replay_metadata_event_log(fixture.context.as_ref())
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(())
}

/// Make the routed metadata authority unreadable, handing back the exact bytes
/// [`restore_config`] puts back: a freshly seeded config would not carry the
/// placement the harvested documents were stamped with.
async fn break_config(fixture: &Fixture) -> Result<Vec<u8>, BoxError> {
    let saved = read_value(
        &fixture.context.storage_handle,
        REALM_CONFIG_KEYSPACE,
        fixture.actor.realm_id.as_bytes().to_vec(),
    )
    .await?
    .ok_or("realm config missing")?;
    write_value(
        &fixture.context.storage_handle,
        REALM_CONFIG_KEYSPACE,
        fixture.actor.realm_id.as_bytes().to_vec(),
        vec![0xff; 8],
    )
    .await?;
    Ok(saved)
}

async fn restore_config(fixture: &Fixture, saved: Vec<u8>) -> Result<(), BoxError> {
    write_value(
        &fixture.context.storage_handle,
        REALM_CONFIG_KEYSPACE,
        fixture.actor.realm_id.as_bytes().to_vec(),
        saved,
    )
    .await
}

async fn record_present(fixture: &Fixture, document_id: Ulid) -> Result<bool, BoxError> {
    Ok(
        load_metadata_record_by_document(&fixture.context, document_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_some(),
    )
}

/// Withdraw the document the way an owner would, straight through the metadata
/// delete path, leaving the harvest provenance behind untouched.
async fn delete_document(fixture: &Fixture, document_id: Ulid) -> Result<(), BoxError> {
    let record = load_metadata_record_by_document(&fixture.context, document_id)
        .await
        .map_err(|error| format!("{error:?}"))?
        .ok_or("document missing before the delete")?;
    delete_metadata_document_routed(
        &fixture.context,
        fixture.actor.clone(),
        Some(&record),
        document_id,
        Some(MetadataAuthToken::internal(AuthContext {
            user_id: fixture.actor.user_id,
            realm_id: fixture.actor.realm_id,
            path_restrictions: None,
        })),
    )
    .await
    .map_err(|error| format!("{error:?}"))?;
    Ok(())
}

async fn run(fixture: &Fixture, source: &HarvestSource) -> JobRunOutcome {
    let (ctx, _cancel, _shutdown) = fixture.job_context();
    run_harvest_job(
        &ctx,
        &HarvestJobSpec {
            source_id: source.source_id,
            group_id: source.group_id,
        },
    )
    .await
}

async fn provenance(
    fixture: &Fixture,
    group_id: GroupId,
    identifier: &str,
) -> Result<Option<HarvestProvenance>, BoxError> {
    let event = fixture
        .context
        .storage_handle
        .send_effect(read_provenance_effect(
            group_id, NAMESPACE, identifier, None,
        ))
        .await;
    Ok(parse_provenance_read(event)?)
}

async fn cursor(fixture: &Fixture, source: &HarvestSource) -> Result<HarvestSource, BoxError> {
    let event = fixture
        .context
        .storage_handle
        .send_effect(read_source_effect(source.group_id, source.source_id, None))
        .await;
    Ok(parse_source_read(event)?.ok_or("source vanished")?)
}

fn expect_write(event: Event) -> Result<(), BoxError> {
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected write event: {other:?}").into()),
    }
}

fn describe(outcome: &JobRunOutcome) -> &'static str {
    match outcome {
        JobRunOutcome::Succeeded(_) => "succeeded",
        JobRunOutcome::Failed(_) => "failed",
        JobRunOutcome::Cancelled => "cancelled",
        JobRunOutcome::Interrupted => "interrupted",
    }
}

fn failure(outcome: &JobRunOutcome) -> &JobError {
    match outcome {
        JobRunOutcome::Failed(error) => error,
        other => panic!("expected a failure, got {}", describe(other)),
    }
}

fn counts(outcome: &JobRunOutcome) -> (u64, u64, u64, u64) {
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::Harvest {
            minted,
            updated,
            tombstoned,
            skipped,
        }) => (*minted, *updated, *tombstoned, *skipped),
        JobRunOutcome::Failed(error) => panic!("expected a harvest result, got {error:?}"),
        other => panic!("expected a harvest result, got {}", describe(other)),
    }
}

// ------------------------------------------------------------------ tests

/// Live, updated, deleted, revived, interrupted and resumed records all land on
/// one current document per source generation.
#[tokio::test]
async fn fixture_harvest_converges() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());

    // Run 1: the first page applies, the second is interrupted mid-flight.
    let (ctx, _cancel, shutdown) = fixture.job_context();
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            Some("T1"),
        ),
    );
    provider.interrupt("T1", shutdown.clone());
    let outcome = run_harvest_job(
        &ctx,
        &HarvestJobSpec {
            source_id: source.source_id,
            group_id: source.group_id,
        },
    )
    .await;
    assert!(
        matches!(outcome, JobRunOutcome::Interrupted),
        "expected an interrupted run, got {}",
        describe(&outcome)
    );
    let alpha = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(alpha.state, HarvestRecordState::Live);
    let alpha_id = alpha.meta_resource_id;
    // A random ULID would not decode, and the create would never resolve a placement.
    assert!(
        MetaResourceId::from_bytes(alpha_id.to_bytes()).is_ok(),
        "harvest must mint a structured metadata id"
    );
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    assert_eq!(
        stored
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.resumption_token.clone()),
        Some("T1".to_string())
    );

    // Run 2: resumes from the persisted token and finishes the listing.
    provider.interrupt("never", CancellationToken::new());
    provider.script(
        "T1",
        list_page(
            &[live_record(BETA, "2026-01-03T00:00:00Z", "Beta v1")],
            None,
        ),
    );
    let outcome = run(&fixture, &stored).await;
    assert_eq!(counts(&outcome), (1, 0, 0, 0));
    let beta = provenance(&fixture, source.group_id, BETA)
        .await?
        .ok_or("beta provenance missing")?;
    let beta_id = beta.meta_resource_id;
    assert_eq!(beta.state, HarvestRecordState::Live);
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    assert!(
        stored
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.resumption_token.is_none())
    );
    assert!(stored.granularity.is_some());

    // Run 3: alpha changes upstream, beta is withdrawn.
    assert!(
        load_metadata_record_by_document(&fixture.context, beta_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_some(),
        "beta must exist before it is withdrawn"
    );
    provider.script(
        "start",
        list_page(
            &[
                live_record(ALPHA, "2026-01-04T00:00:00Z", "Alpha v2"),
                deleted_record(BETA, "2026-01-05T00:00:00Z"),
            ],
            None,
        ),
    );
    let outcome = run(&fixture, &stored).await;
    assert_eq!(counts(&outcome), (0, 1, 1, 0));
    let alpha = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(alpha.meta_resource_id, alpha_id, "update must not remap");
    let beta = provenance(&fixture, source.group_id, BETA)
        .await?
        .ok_or("beta provenance missing")?;
    assert_eq!(beta.state, HarvestRecordState::Tombstoned);
    assert!(
        load_metadata_record_by_document(&fixture.context, beta_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_none(),
        "tombstoned document must be gone"
    );

    // Run 4: beta reappears and gets a successor identity, never the old one.
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    provider.script(
        "start",
        list_page(
            &[live_record(BETA, "2026-01-06T00:00:00Z", "Beta reborn")],
            None,
        ),
    );
    let outcome = run(&fixture, &stored).await;
    assert_eq!(counts(&outcome), (1, 0, 0, 0));
    let revived = provenance(&fixture, source.group_id, BETA)
        .await?
        .ok_or("beta provenance missing")?;
    assert_eq!(revived.state, HarvestRecordState::Live);
    assert_ne!(revived.meta_resource_id, beta_id);
    assert_eq!(revived.predecessors, vec![beta_id]);
    assert!(
        load_metadata_record_by_document(&fixture.context, beta_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .is_none(),
        "the withdrawn identity must stay withdrawn"
    );

    // Run 5: replaying the same page changes nothing.
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    let outcome = run(&fixture, &stored).await;
    assert_eq!(counts(&outcome), (0, 0, 0, 1));
    let replayed = provenance(&fixture, source.group_id, BETA)
        .await?
        .ok_or("beta provenance missing")?;
    assert_eq!(replayed.meta_resource_id, revived.meta_resource_id);

    // One current document per source generation.
    for id in [alpha.meta_resource_id, revived.meta_resource_id] {
        let record = load_metadata_record_by_document(&fixture.context, id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .ok_or("current document missing")?;
        assert_eq!(record.group_id, source.group_id);
        assert!(record.document_path.starts_with(PREFIX));
    }

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A crash between the identity write and the create leaves a `PendingCreate`
/// row; the replay must adopt that id rather than mint a second one.
#[tokio::test]
async fn pending_identity_adopted() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            None,
        ),
    );

    // Run once to obtain a real structured id, then rewind to the crash point.
    let outcome = run(&fixture, &source).await;
    assert_eq!(counts(&outcome), (1, 0, 0, 0));
    let minted = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    let pending = HarvestProvenance {
        state: HarvestRecordState::PendingCreate,
        ..minted.clone()
    };
    expect_write(
        fixture
            .context
            .storage_handle
            .send_effect(
                aruna_operations::harvest::repository::write_provenance_effect(&pending, None)?,
            )
            .await,
    )?;

    // The document already exists, so the replay resolves the identity in place.
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    let outcome = run(&fixture, &stored).await;
    assert_eq!(counts(&outcome), (1, 0, 0, 0));
    let resumed = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(resumed.meta_resource_id, minted.meta_resource_id);
    assert_eq!(resumed.state, HarvestRecordState::Live);

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// Two groups harvesting the same namespace and identifier must not share a
/// provenance row, and must mint independent documents.
#[tokio::test]
async fn groups_separate_provenance() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            None,
        ),
    );

    let first = seed_source(&fixture, &provider.endpoint).await?;
    let other_group = Ulid::generate();
    seed_auth(&fixture.context.storage_handle, &fixture.actor, other_group).await?;
    let second = HarvestSource::new(
        Ulid::generate(),
        other_group,
        first.connector_id,
        NAMESPACE.to_string(),
        PREFIX.to_string(),
        HarvestSelector::default(),
        None,
        SystemTime::now(),
        fixture.actor.user_id,
    );
    let mut connector_for_second = RepositoryConnector::new(
        first.connector_id,
        other_group,
        "fixture".to_string(),
        RepositoryConnectorKind::OaiPmh,
        provider.endpoint.clone(),
        HashMap::new(),
        SystemTime::now(),
        SystemTime::now(),
        fixture.actor.user_id,
    );
    connector_for_second.connector_id = first.connector_id;
    for (key_space, key, value) in connector_writes(&connector_for_second, None)? {
        write_value(
            &fixture.context.storage_handle,
            &key_space,
            key.as_ref().to_vec(),
            value.as_ref().to_vec(),
        )
        .await?;
    }
    expect_write(
        fixture
            .context
            .storage_handle
            .send_effect(write_source_effect(&second, None)?)
            .await,
    )?;

    assert_eq!(counts(&run(&fixture, &first).await), (1, 0, 0, 0));
    assert_eq!(counts(&run(&fixture, &second).await), (1, 0, 0, 0));

    let left = provenance(&fixture, first.group_id, ALPHA)
        .await?
        .ok_or("first provenance missing")?;
    let right = provenance(&fixture, second.group_id, ALPHA)
        .await?
        .ok_or("second provenance missing")?;
    assert_ne!(left.meta_resource_id, right.meta_resource_id);
    assert_eq!(left.group_id, first.group_id);
    assert_eq!(right.group_id, second.group_id);

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// An upstream deletion delivered while the create is still unprojected, or
/// while the routed authority is unreachable, must not retire the identity: the
/// document is confirmed gone first, and replay converges without an orphan.
#[tokio::test]
async fn deletion_awaits_confirmation() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            None,
        ),
    );

    // Create and deliberately leave the registry projection behind.
    assert_eq!(counts(&run(&fixture, &source).await), (1, 0, 0, 0));
    let created = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    let alpha_id = created.meta_resource_id;
    assert_eq!(created.state, HarvestRecordState::Live);
    assert!(
        !record_present(&fixture, alpha_id).await?,
        "the local registry projection must still lag the create"
    );

    // The upstream withdrawal arrives during that lag. A fresh job context each
    // time covers both the immediate retry and a restarted worker.
    provider.script(
        "start",
        list_page(&[deleted_record(ALPHA, "2026-01-05T00:00:00Z")], None),
    );
    for attempt in 0..2 {
        let outcome = run(&fixture, &source).await;
        let error = failure(&outcome);
        assert_eq!(
            error.kind,
            JobErrorKind::Retryable,
            "attempt {attempt} must retry: {error:?}"
        );
        assert!(
            error.message.contains("pending registry projection"),
            "attempt {attempt} unexpected failure: {error:?}"
        );
        assert_eq!(
            provenance(&fixture, source.group_id, ALPHA)
                .await?
                .ok_or("alpha provenance missing")?
                .state,
            HarvestRecordState::Live,
            "provenance must not go terminal before the document is gone"
        );
    }

    // Projection caught up, but the routed authority is unavailable.
    drain(&fixture).await?;
    assert!(record_present(&fixture, alpha_id).await?);
    let saved_config = break_config(&fixture).await?;
    let outcome = run(&fixture, &source).await;
    let error = failure(&outcome);
    assert_eq!(
        error.kind,
        JobErrorKind::Retryable,
        "an unavailable authority must retry: {error:?}"
    );
    assert_eq!(
        provenance(&fixture, source.group_id, ALPHA)
            .await?
            .ok_or("alpha provenance missing")?
            .state,
        HarvestRecordState::Live
    );
    assert!(
        record_present(&fixture, alpha_id).await?,
        "a failed withdrawal must leave the document alone"
    );

    // Authority restored: the same delivery now retires the identity.
    restore_config(&fixture, saved_config).await?;
    assert_eq!(counts(&run(&fixture, &source).await), (0, 0, 1, 0));
    let tombstoned = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(tombstoned.state, HarvestRecordState::Tombstoned);
    assert!(
        !record_present(&fixture, alpha_id).await?,
        "a tombstoned identity must have no live document"
    );

    // Replaying the withdrawal changes nothing.
    let stored = cursor(&fixture, &source).await?;
    assert_eq!(counts(&run(&fixture, &stored).await), (0, 0, 0, 1));

    // Revival allocates a successor and leaves no orphan behind it.
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-07T00:00:00Z", "Alpha reborn")],
            None,
        ),
    );
    assert_eq!(counts(&run(&fixture, &stored).await), (1, 0, 0, 0));
    let revived = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(revived.state, HarvestRecordState::Live);
    assert_ne!(revived.meta_resource_id, alpha_id);
    assert_eq!(revived.predecessors, vec![alpha_id]);
    assert!(!record_present(&fixture, alpha_id).await?);
    drain(&fixture).await?;
    assert!(record_present(&fixture, revived.meta_resource_id).await?);

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A document deleted out of band while its provenance stays live must not wedge
/// the source: an update can never land on what is gone, so the next upstream
/// change retires that identity and mints a successor. A create still queued for
/// projection is not the same condition and only waits.
#[tokio::test]
async fn deleted_document_revives() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            None,
        ),
    );
    assert_eq!(counts(&run(&fixture, &source).await), (1, 0, 0, 0));
    let created = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    let alpha_id = created.meta_resource_id;

    // The create is not projected here yet, so an update in that window waits
    // instead of mistaking the lag for a deletion.
    assert!(!record_present(&fixture, alpha_id).await?);
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-04T00:00:00Z", "Alpha v2")],
            None,
        ),
    );
    let outcome = run(&fixture, &source).await;
    let error = failure(&outcome);
    assert_eq!(error.kind, JobErrorKind::Retryable, "{error:?}");
    assert!(
        error.message.contains("pending registry projection"),
        "unexpected failure: {error:?}"
    );
    assert_eq!(
        provenance(&fixture, source.group_id, ALPHA)
            .await?
            .ok_or("alpha provenance missing")?
            .meta_resource_id,
        alpha_id
    );

    // The owner deletes the document; provenance still says live.
    drain(&fixture).await?;
    assert!(record_present(&fixture, alpha_id).await?);
    delete_document(&fixture, alpha_id).await?;
    drain(&fixture).await?;
    assert!(!record_present(&fixture, alpha_id).await?);
    assert_eq!(
        provenance(&fixture, source.group_id, ALPHA)
            .await?
            .ok_or("alpha provenance missing")?
            .state,
        HarvestRecordState::Live
    );

    // The upstream change now lands on a successor identity.
    let stored = cursor(&fixture, &source).await?;
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-06T00:00:00Z", "Alpha v3")],
            None,
        ),
    );
    assert_eq!(counts(&run(&fixture, &stored).await), (1, 0, 0, 0));
    let revived = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(revived.state, HarvestRecordState::Live);
    assert_ne!(revived.meta_resource_id, alpha_id);
    assert_eq!(revived.predecessors, vec![alpha_id]);
    drain(&fixture).await?;
    assert!(record_present(&fixture, revived.meta_resource_id).await?);
    assert!(!record_present(&fixture, alpha_id).await?);

    // Replaying the same page converges instead of failing again.
    let stored = cursor(&fixture, &source).await?;
    assert_eq!(counts(&run(&fixture, &stored).await), (0, 0, 0, 1));

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A registry row durable before its graph materializes must never retire the
/// source record. The harvest update path does not read the graph today, so the
/// update lands straight away; if it ever does surface a missing graph, that
/// failure has to stay retryable and the next run must still update the same
/// identity. The classifier table test pins the retryability itself down.
#[tokio::test]
async fn unmaterialized_graph_retries() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            None,
        ),
    );
    assert_eq!(counts(&run(&fixture, &source).await), (1, 0, 0, 0));
    let created = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;

    // Registry visible, no graph materialized.
    project_only(&fixture).await?;
    assert!(record_present(&fixture, created.meta_resource_id).await?);
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-04T00:00:00Z", "Alpha v2")],
            None,
        ),
    );
    let stored = cursor(&fixture, &source).await?;
    match run(&fixture, &stored).await {
        JobRunOutcome::Failed(error) => {
            assert_eq!(
                error.kind,
                JobErrorKind::Retryable,
                "a lagging graph must not retire the record: {error:?}"
            );
            // Materialization catches up and the retry applies the same record.
            drain(&fixture).await?;
            let stored = cursor(&fixture, &source).await?;
            assert_eq!(counts(&run(&fixture, &stored).await), (0, 1, 0, 0));
        }
        applied => {
            assert_eq!(counts(&applied), (0, 1, 0, 0));
            drain(&fixture).await?;
        }
    }

    let updated = provenance(&fixture, source.group_id, ALPHA)
        .await?
        .ok_or("alpha provenance missing")?;
    assert_eq!(updated.state, HarvestRecordState::Live);
    assert_eq!(updated.meta_resource_id, created.meta_resource_id);
    assert!(updated.source_datestamp_ms > created.source_datestamp_ms);
    assert!(record_present(&fixture, created.meta_resource_id).await?);

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A provider handing back a token it already issued is a cycle: the run ends
/// retryable with the token cleared instead of paging forever.
#[tokio::test]
async fn repeated_token_ends() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            Some("T1"),
        ),
    );
    provider.script(
        "T1",
        list_page(
            &[live_record(BETA, "2026-01-03T00:00:00Z", "Beta v1")],
            Some("T1"),
        ),
    );

    let outcome = run(&fixture, &source).await;
    let error = failure(&outcome);
    assert!(
        error.message.contains("resumption token twice"),
        "unexpected failure: {error:?}"
    );
    assert!(
        error.kind == JobErrorKind::Retryable,
        "token cycles must retry"
    );
    drain(&fixture).await?;
    let stored = cursor(&fixture, &source).await?;
    assert!(
        stored
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.resumption_token.is_none())
    );

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A `badResumptionToken` restarts the window once inside the run; a provider
/// that keeps rejecting cannot restart-loop.
#[tokio::test]
async fn expired_token_restarts() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script(
        "start",
        list_page(
            &[live_record(ALPHA, "2026-01-02T00:00:00Z", "Alpha v1")],
            Some("T1"),
        ),
    );
    provider.script("T1", error_page("badResumptionToken", "expired"));

    let outcome = run(&fixture, &source).await;
    let error = failure(&outcome);
    assert!(
        error.kind == JobErrorKind::Retryable,
        "restart exhaustion must retry"
    );
    let keys = provider.requests();
    assert_eq!(
        keys.iter().filter(|key| key.as_str() == "start").count(),
        2,
        "exactly one restart: {keys:?}"
    );
    assert_eq!(
        keys.iter().filter(|key| key.as_str() == "T1").count(),
        2,
        "the restarted listing re-reaches the same token: {keys:?}"
    );

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// Malformed upstream XML is transient junk on a fresh request and must retry;
/// an OAI protocol rejection of that same request is a configuration fault.
#[tokio::test]
async fn parse_failures_classified() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());

    provider.script("start", "<OAI-PMH><ListRecords></Mismatched></OAI-PMH>");
    assert!(
        failure(&run(&fixture, &source).await).kind == JobErrorKind::Retryable,
        "malformed XML must retry"
    );

    provider.script("start", error_page("badArgument", "no such set"));
    assert!(
        failure(&run(&fixture, &source).await).kind == JobErrorKind::Permanent,
        "a protocol rejection is permanent"
    );

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A response larger than the decoded-byte cap is rejected instead of buffered.
#[tokio::test]
async fn oversized_body_rejected() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let (provider, server) = serve_oai().await?;
    let source = seed_source(&fixture, &provider.endpoint).await?;
    provider.script("identify", identify_page());
    provider.script("start", "x".repeat(33 * 1024 * 1024));

    let outcome = run(&fixture, &source).await;
    let error = failure(&outcome);
    assert!(
        error.message.contains("byte cap"),
        "unexpected failure: {error:?}"
    );
    assert_eq!(error.kind, JobErrorKind::Permanent);

    server.abort();
    fixture.stop().await;
    Ok(())
}

/// A provider that never answers must not hold the worker past a cancel: the
/// job's own signals win over the client's inactivity timeouts.
#[tokio::test]
async fn slow_provider_cancellable() -> Result<(), BoxError> {
    let fixture = build_fixture().await?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let stall = tokio::spawn(async move {
        // Accept and never respond.
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let source = seed_source(&fixture, &format!("http://{address}/oai")).await?;

    let (ctx, cancel, _shutdown) = fixture.job_context();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel.cancel();
    });
    let started = tokio::time::Instant::now();
    let outcome = run_harvest_job(
        &ctx,
        &HarvestJobSpec {
            source_id: source.source_id,
            group_id: source.group_id,
        },
    )
    .await;
    assert!(
        matches!(outcome, JobRunOutcome::Cancelled),
        "expected cancellation, got {}",
        describe(&outcome)
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "cancellation must not wait for the fetch deadline"
    );

    stall.abort();
    fixture.stop().await;
    Ok(())
}
