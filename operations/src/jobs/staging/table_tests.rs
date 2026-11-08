//! Runs table imports against a local HTTP server with NCBI style index pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use aruna_core::structs::execution::job::{
    JobClaim, JobId, JobPayload, JobProgress, JobRecord, JobResultPayload, StagingJobPrefix,
};
use aruna_core::structs::execution::table::{SEARCH_EXACT_KEY, TableImport};
use aruna_core::structs::storage::origin::VersionOrigin;
use axum::body::Body;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::*;
use crate::jobs::executor::{JobRunOutcome, ProgressReporter};
use crate::s3::object::search::{ObjectKeyMatch, SearchObjectsInput, search_local_objects};

const SUMMARY: &str = include_str!("../../../../core/tests/fixtures/assembly_summary.tsv");
const COLI: &str = "all/GCF/000/005/845/GCF_000005845.2_ASM584v2/";
const HUMAN: &str = "all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/";
const STRUCTURE: &str = "GCF_000001405.40_GRCh38.p14_assembly_structure/";

/// Files and index pages below `/genomes/`, and every request the server saw.
#[derive(Default)]
struct Site {
    files: BTreeMap<String, Vec<u8>>,
    requests: Mutex<Vec<(Method, String)>>,
    /// Answers every request with 429 and a one hour `Retry-After`.
    throttled: bool,
}

impl Site {
    fn ncbi(endpoint: &str) -> Self {
        let base = format!("{endpoint}/genomes/");
        let summary = SUMMARY.replace("https://ftp.ncbi.nlm.nih.gov/genomes/", &base);
        let mut files = BTreeMap::new();
        files.insert(
            "ASSEMBLY_REPORTS/assembly_summary_refseq.txt".to_string(),
            summary.into_bytes(),
        );
        files.insert(
            format!("{COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz"),
            b"ACGT".to_vec(),
        );
        files.insert(format!("{COLI}md5checksums.txt"), b"sums".to_vec());
        files.insert(format!("{HUMAN}README.txt"), b"readme".to_vec());
        files.insert(format!("{HUMAN}{STRUCTURE}chr1.fna"), b"NNNN".to_vec());
        files.insert(
            "all/GCF/000/006/945/GCF_000006945.2_ASM694v2/x.txt".to_string(),
            b"x".to_vec(),
        );
        Self {
            files,
            requests: Mutex::default(),
            throttled: false,
        }
    }

    fn count(&self, method: &Method, path: &str) -> usize {
        let requests = self.requests.lock().unwrap();
        requests
            .iter()
            .filter(|(seen, at)| seen == method && at == path)
            .count()
    }

    /// An Apache style index of the direct children of `directory`, sizes rounded as NCBI does.
    fn index(&self, directory: &str) -> Option<String> {
        let mut rows = Vec::new();
        for path in self.files.keys() {
            let Some(rest) = path.strip_prefix(directory) else {
                continue;
            };
            let row = match rest.split_once('/') {
                Some((folder, _)) => {
                    format!("<a href=\"{folder}/\">{folder}/</a>  2025-11-17 12:38    -")
                }
                None => format!("<a href=\"{rest}\">{rest}</a>  2025-11-17 12:38  1.0K"),
            };
            if !rows.contains(&row) {
                rows.push(row);
            }
        }
        (!rows.is_empty()).then(|| {
            format!(
                "<html><head><title>Index of /genomes/{directory}</title></head><body>\
                 <h1>Index of /genomes/{directory}</h1><pre><a href=\"../\">Parent Directory</a>\n{}\
                 \n</pre></body></html>",
                rows.join("\n")
            )
        })
    }

    fn answer(&self, method: Method, uri: &Uri, headers: &HeaderMap) -> Response {
        let path = uri
            .path()
            .strip_prefix("/genomes/")
            .unwrap_or_default()
            .to_string();
        self.requests
            .lock()
            .unwrap()
            .push((method.clone(), path.clone()));
        if self.throttled {
            return Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .header("retry-after", "3600")
                .body(Body::empty())
                .unwrap();
        }
        let Some(bytes) = self.files.get(&path) else {
            return match self
                .index(&path)
                .filter(|_| path.is_empty() || path.ends_with('/'))
            {
                Some(html) => Response::builder()
                    .header("content-type", "text/html")
                    .body(Body::from(html))
                    .unwrap(),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap(),
            };
        };
        let range = (headers.get("range").and_then(|value| value.to_str().ok()))
            .and_then(|value| value.strip_prefix("bytes="))
            .and_then(|value| value.split_once('-'))
            .map(|(start, end)| {
                let start: usize = start.parse().unwrap_or(0);
                let end = end
                    .parse::<usize>()
                    .map_or(bytes.len(), |end| (end + 1).min(bytes.len()));
                (start, end)
            });
        let modified = "Mon, 17 Nov 2025 17:38:21 GMT";
        let builder = Response::builder().header("last-modified", modified);
        match (method, range) {
            (Method::HEAD, _) => builder
                .header("content-length", bytes.len())
                .body(Body::empty())
                .unwrap(),
            (_, Some((start, end))) => builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    "content-range",
                    format!("bytes {start}-{}/{}", end - 1, bytes.len()),
                )
                .body(Body::from(bytes[start..end].to_vec()))
                .unwrap(),
            (_, None) => builder.body(Body::from(bytes.clone())).unwrap(),
        }
    }
}

async fn serve(
    site: impl FnOnce(&str) -> Site,
) -> (String, Arc<Site>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let site = Arc::new(site(&endpoint));
    let shared = site.clone();
    let answer = move |method: Method, uri: Uri, headers: HeaderMap| {
        let site = shared.clone();
        async move { site.answer(method, &uri, &headers) }
    };
    let router = axum::Router::new().fallback(answer);
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (endpoint, site, task)
}

struct Fixture {
    driver: Arc<crate::driver::DriverContext>,
    spec: StagingJobSpec,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new(endpoint: &str, table: serde_json::Value) -> Self {
        use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
        use crate::tests::staging::{create_test_bucket, setup_driver_context};
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;

        let test = setup_driver_context().await;
        let driver = test.driver_context;
        let mut spec = crate::jobs::staging::pure_tests::test_spec();
        spec.prefixes = Vec::<StagingJobPrefix>::new();
        spec.table = Some(serde_json::from_value::<TableImport>(table).unwrap());
        let input = SourceConnectorInput {
            group_id: spec.group_id,
            created_by: Default::default(),
            name: "ncbi".to_string(),
            kind: SourceConnectorKind::Http,
            public_config: std::collections::HashMap::from([
                ("endpoint".to_string(), endpoint.to_string()),
                ("root".to_string(), "genomes".to_string()),
            ]),
            secret_config: Default::default(),
        };
        let connector = drive(SourceConnectorOperation::new(input), &driver)
            .await
            .unwrap();
        spec.connector_id = connector.connector.connector_id;
        let scope = (spec.auth_context.realm_id, spec.group_id, spec.node_id);
        spec.auth_context = crate::tests::access::seed_access(&driver, scope, "/none").await;
        let owner = spec.auth_context.user_id;
        for name in ["lists", &spec.bucket] {
            let bucket = create_test_bucket(&driver, spec.group_id, owner, name).await;
            if name == spec.bucket {
                spec.bucket_identity = bucket.identity();
            }
        }
        Self {
            driver: Arc::new(driver),
            spec,
            _dir: test._tempdir,
        }
    }

    async fn context(&self, seed: u8) -> JobContext {
        self.context_for(&self.spec, seed).await
    }

    /// A claimed job of `spec`.
    async fn context_for(&self, spec: &StagingJobSpec, seed: u8) -> JobContext {
        let (job_id, claim_token) = (JobId::from_bytes([seed; 16]), Ulid::from_bytes([seed; 16]));
        let owner = spec.auth_context.user_id;
        let payload = JobPayload::Staging(spec.clone());
        let mut record = JobRecord::new(job_id, payload, owner, spec.node_id, 1, 1, None);
        record.claim = Some(JobClaim {
            holder_node_id: spec.node_id,
            claim_token,
            lease_expires_ms: u64::MAX,
        });
        let storage = &self.driver.storage_handle;
        crate::jobs::store::insert_job(storage, &record)
            .await
            .unwrap();
        JobContext {
            driver: self.driver.clone(),
            job_id,
            owner_node_id: self.spec.node_id,
            claim_token,
            final_attempt: false,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("items")),
            range_turns: Default::default(),
            mail: Default::default(),
        }
    }

    async fn run(&self, seed: u8) -> (u64, u64) {
        let ctx = self.context(seed).await;
        match crate::jobs::staging::run_staging_job(&ctx, &self.spec).await {
            JobRunOutcome::Succeeded(JobResultPayload::Staging {
                completed_items,
                failed_items,
            }) => (completed_items, failed_items),
            JobRunOutcome::Failed(error) => panic!("the table import failed: {error:?}"),
            _ => panic!("the table import did not finish"),
        }
    }

    async fn head(
        &self,
        bucket: &str,
        key: &str,
    ) -> Option<(Ulid, std::collections::HashMap<String, String>)> {
        let input = HeadObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
        };
        let head = drive(HeadObjectOperation::new(input), &self.driver)
            .await
            .ok()?;
        Some((head.version_id?, head.metadata))
    }

    async fn search(&self, query: &str) -> Vec<String> {
        let hits = self.hits(query).await;
        hits.into_iter().map(|hit| hit.key).collect()
    }

    async fn hits(&self, query: &str) -> Vec<crate::s3::object::search::ObjectInventoryHit> {
        let auth = self.spec.auth_context.clone();
        let input = SearchObjectsInput {
            realm_id: auth.realm_id,
            auth,
            node_id: self.spec.node_id,
            query: query.to_string(),
            key_match: ObjectKeyMatch::Metadata,
            bucket: Some(self.spec.bucket.clone()),
            limit: 100,
            start_after: None,
            as_of: SystemTime::now() + std::time::Duration::from_secs(3600),
        };
        let page = search_local_objects(&self.driver, input).await.unwrap();
        page.hits.into_iter().map(|hit| hit.hit).collect()
    }

    /// The source observation stored on the current reference version of `key`.
    async fn observation(
        &self,
        key: &str,
    ) -> aruna_core::structs::execution::source_access::SourceMetadata {
        use aruna_core::structs::storage::blob::BlobVersionState;
        match self.version(key).await.state {
            BlobVersionState::Reference {
                cached_metadata, ..
            } => cached_metadata,
            _ => panic!("the version must be a reference"),
        }
    }

    /// The current version of `key` in the job's bucket.
    async fn version(&self, key: &str) -> aruna_core::structs::storage::blob::BlobVersion {
        use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
        use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
        let bucket = self.spec.bucket.clone();
        let (version_id, _) = self.head(&bucket, key).await.unwrap();
        let read = aruna_core::effects::StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(&bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        };
        let event = self.driver.storage_handle.send_storage_effect(read).await;
        let aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::ReadResult {
            value: Some(value),
            ..
        }) = event
        else {
            panic!("the version must exist");
        };
        BlobVersion::from_bytes(&value).unwrap()
    }

    /// The origin of the current version of `key`.
    async fn origin(
        &self,
        key: &str,
    ) -> Option<aruna_core::structs::storage::origin::VersionOrigin> {
        self.version(key).await.origin
    }

    /// The whole object read through the ordinary read, or why it was refused.
    async fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        use crate::s3::object::get::{GetObjectInput, GetObjectOperation};
        use futures_util::StreamExt;
        let auth = &self.spec.auth_context;
        let input = GetObjectInput {
            bucket: self.spec.bucket.clone(),
            key: key.to_string(),
            version_id: None,
            range: None,
            group_id: self.spec.group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: self.spec.node_id,
        };
        let operation = GetObjectOperation::new(input);
        let mut blob = (drive(operation, &self.driver).await)
            .map_err(|error| error.to_string())?
            .blob;
        let mut bytes = Vec::new();
        while let Some(chunk) = blob.next().await {
            bytes.extend_from_slice(&chunk.map_err(|error| error.to_string())?);
        }
        Ok(bytes)
    }
}

fn summary_table() -> serde_json::Value {
    serde_json::json!({
        "source": { "connector": "ASSEMBLY_REPORTS/assembly_summary_refseq.txt" },
        "format": { "delimiter": "\t", "header": "last_comment", "comment": "#",
                    "nulls": ["na", ""] },
        "rows": {
            "kind": "folder",
            "source": "{ftp_path}",
            "target": "{ftp_path|basename}",
            "filter": [{ "column": "assembly_accession",
                         "any_of": ["GCF_000005845.2", "GCF_000001405.40"] }],
            "text": ["assembly_accession", "organism_name", "biosample"],
            "exact": ["taxid", "species_taxid", "assembly_level", "group"]
        },
        "rate": 1000
    })
}

// The two fixture assemblies become folder objects with their summary columns; their files,
// subfolders included, become references with one HEAD each; a re-run writes nothing.
#[tokio::test]
async fn summary_imports_folders() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let fixture = Fixture::new(&endpoint, summary_table()).await;

    assert_eq!(fixture.run(7).await, (4, 0));
    let bucket = fixture.spec.bucket.clone();
    let folder = "GCF_000005845.2_ASM584v2/";
    let (folder_version, metadata) = fixture.head(&bucket, folder).await.unwrap();
    assert_eq!(metadata["taxid"], "511145");
    let job_id = Some(JobId::from_bytes([7; 16]).as_ulid());
    let origin = fixture.origin(folder).await;
    assert_eq!(origin, Some(VersionOrigin::Source { job_id }));
    assert_eq!(
        metadata["organism_name"],
        "Escherichia coli str. K-12 substr. MG1655"
    );
    assert!(!metadata.contains_key("wgs_master"));
    assert_eq!(
        metadata[SEARCH_EXACT_KEY],
        "taxid,species_taxid,assembly_level,group"
    );
    let genome = format!("{folder}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    let (genome_version, genome_metadata) = fixture.head(&bucket, &genome).await.unwrap();
    assert!(genome_metadata.is_empty());
    let nested = format!("GCF_000001405.40_GRCh38.p14/{STRUCTURE}chr1.fna");
    assert!(fixture.head(&bucket, &nested).await.is_some());
    assert!(
        fixture
            .head(&bucket, "GCF_000006945.2_ASM694v2/")
            .await
            .is_none()
    );
    // Index pages round sizes, so every file got its observation from a HEAD.
    let file = format!("{COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    assert_eq!(site.count(&Method::HEAD, &file), 1);

    assert_eq!(fixture.run(8).await, (4, 0));
    assert_eq!(
        fixture.head(&bucket, folder).await.unwrap().0,
        folder_version
    );
    assert_eq!(
        fixture.head(&bucket, &genome).await.unwrap().0,
        genome_version
    );

    // An ordinary bucket matches metadata during its head scan; tax ids match only exactly.
    assert_eq!(
        fixture.search("escherichia species_taxid=562").await,
        [folder]
    );
    assert_eq!(fixture.search("coli taxid=562").await, Vec::<String>::new());
    let complete = r#"assembly_level="Complete Genome""#;
    assert_eq!(fixture.search(complete).await, [folder]);
    assert_eq!(
        fixture.search("sapiens").await,
        ["GCF_000001405.40_GRCh38.p14/"]
    );
    server.abort();
}

// A gzip table spanning many chunks is fetched once per run: each chunk continues the decoder
// of the one before instead of decompressing the table from its start again.
#[tokio::test]
async fn gzip_table_streams() {
    use std::io::Write;
    const GZIP: &str = "ASSEMBLY_REPORTS/assembly_summary_refseq.txt.gz";
    let (endpoint, site, server) = serve(|endpoint| {
        let mut site = Site::ncbi(endpoint);
        let summary = String::from_utf8(site.files[GZIP.trim_end_matches(".gz")].clone()).unwrap();
        let (header, rows) = summary.split_at(summary.find("\nGCF_").unwrap() + 1);
        // A filtered row repeated until the real rows lie several chunks into the table.
        let filler = rows.lines().nth(2).unwrap();
        let mut table = header.to_string();
        while table.len() < 4 * aruna_core::structs::execution::table::TABLE_CHUNK {
            table.push_str(filler);
            table.push('\n');
        }
        table.push_str(rows);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Default::default());
        encoder.write_all(table.as_bytes()).unwrap();
        site.files
            .insert(GZIP.to_string(), encoder.finish().unwrap());
        site
    })
    .await;
    let mut table = summary_table();
    table["source"]["connector"] = GZIP.into();
    let fixture = Fixture::new(&endpoint, table).await;

    assert_eq!(fixture.run(9).await, (4, 0));
    assert_eq!(
        site.count(&Method::GET, GZIP),
        1,
        "one stream for the whole table"
    );
    server.abort();
}

/// A stream of `bytes` in pieces, the way a transport delivers them.
fn pieces(bytes: Vec<u8>) -> BackendStream<Result<Bytes, StreamError>> {
    let pieces: Vec<Result<Bytes, StreamError>> = (bytes.chunks(16 * 1024))
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    BackendStream(Box::pin(futures_util::stream::iter(pieces)))
}

// A source rewritten in place while a step kept its stream makes the kept decoder fail; the
// stream is opened again once, and that open reports the change, never a broken table.
#[tokio::test]
async fn rewritten_gzip_reopens() {
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut seed = 7u64;
    let mut text = Vec::new();
    while text.len() < 1 << 20 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        text.extend_from_slice(format!("{:016x}\n", seed).as_bytes());
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Default::default());
    encoder.write_all(&text).unwrap();
    let compressed = encoder.finish().unwrap();
    // The first part of the table, then bytes another writer left behind.
    let mut rewritten = compressed[..compressed.len() / 2].to_vec();
    rewritten.extend(std::iter::repeat_n(0xFF, compressed.len() / 2));
    let opens = AtomicUsize::new(0);
    let open = || {
        let first = opens.fetch_add(1, Ordering::SeqCst) == 0;
        let rewritten = rewritten.clone();
        async move {
            match first {
                true => Ok((pieces(rewritten), None)),
                false => Err(ChunkFailure::Changed),
            }
        }
    };
    let mut kept = None;
    let mut offset = 0;
    let failure = loop {
        match gzip_chunk(&mut kept, (None, None, offset), TABLE_CHUNK, open).await {
            Ok((bytes, _, _)) => offset += bytes.len() as u64,
            Err(failure) => break failure,
        }
    };
    assert!(offset > 0, "the first steps read the table");
    assert!(matches!(failure, ChunkFailure::Changed));
    assert_eq!(opens.load(Ordering::SeqCst), 2);

    // A freshly opened stream that is no valid gzip stays a lasting failure.
    let broken = || async { Ok((pieces(vec![0xFF; 1024]), None)) };
    let failure = gzip_chunk(&mut None, (None, None, 0), TABLE_CHUNK, broken).await;
    assert!(matches!(failure, Err(ChunkFailure::Lasting(_))));
}

// An uploaded CSV of file URLs imports one reference per row with its kept columns, read as
// the submitter.
#[tokio::test]
async fn uploaded_list_imports() {
    let (endpoint, _site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "samples.csv" } },
        "rows": { "kind": "file", "source": "{url}", "target": "samples/{url|basename}",
                  "keep": ["sample", "description"], "text": ["description"],
                  "exact": ["sample"] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let csv = format!(
        "url,sample,description\n\
         {endpoint}/genomes/{COLI}md5checksums.txt,s1,\"checksums, coli\"\n\
         {HUMAN}README.txt,s2,human readme\n\
         {HUMAN}README.txt,s2,human readme\n\
         {HUMAN}README.txt,s4,other readme\n\
         https://elsewhere.example.org/x.txt,s3,outside\n"
    );
    let ctx = fixture.context(9).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await
    .unwrap();
    let body = (Bytes::from(csv), &BTreeMap::new());
    write_object(&ctx, &fixture.spec, ("lists", &lists), "samples.csv", body)
        .await
        .ok()
        .unwrap();

    // A repeated row imports once; a row mapping its key differently fails.
    assert_eq!(fixture.run(10).await, (2, 2));
    let bucket = fixture.spec.bucket.clone();
    let (_, metadata) = fixture
        .head(&bucket, "samples/md5checksums.txt")
        .await
        .unwrap();
    assert_eq!(metadata["sample"], "s1");
    assert_eq!(metadata["description"], "checksums, coli");
    let (_, readme) = fixture.head(&bucket, "samples/README.txt").await.unwrap();
    assert_eq!(readme["sample"], "s2");
    assert_eq!(
        fixture.search("readme sample=s2").await,
        ["samples/README.txt"]
    );
    server.abort();
}

// A job cancelled or shut down while its source's `Retry-After` pauses it ends with that stop,
// not with a failed attempt.
#[tokio::test]
async fn paused_job_stops() {
    let throttled = |endpoint: &str| Site {
        throttled: true,
        ..Site::ncbi(endpoint)
    };
    let (endpoint, _site, server) = serve(throttled).await;
    let fixture = Fixture::new(&endpoint, summary_table()).await;
    for (seed, cancel) in [(20u8, true), (21u8, false)] {
        let ctx = fixture.context(seed).await;
        let stop = match cancel {
            true => ctx.cancel.clone(),
            false => ctx.shutdown.clone(),
        };
        let stopping = async {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            stop.cancel();
        };
        let run = crate::jobs::staging::run_staging_job(&ctx, &fixture.spec);
        let (outcome, ()) = tokio::join!(run, stopping);
        match cancel {
            true => assert!(matches!(outcome, JobRunOutcome::Cancelled)),
            false => assert!(matches!(outcome, JobRunOutcome::Interrupted)),
        }
    }
    server.abort();
}

// A shutdown during the cleanup of a permanent failure leaves it failed, since its queue rows
// are gone; a retryable failure under shutdown is still interrupted.
#[tokio::test]
async fn shutdown_keeps_permanent() {
    use aruna_core::structs::execution::job::{JobError, JobErrorKind};
    let fixture = Fixture::new("http://127.0.0.1:9", summary_table()).await;
    let ctx = fixture.context(22).await;
    ctx.shutdown.cancel();
    let stopped = |error| crate::jobs::staging::stopped_outcome(&ctx, JobRunOutcome::Failed(error));
    let permanent = stopped(JobError::permanent("bucket replaced")).await;
    assert!(
        matches!(permanent, JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Permanent)
    );
    let retryable = stopped(JobError::retryable("reset")).await;
    assert!(matches!(retryable, JobRunOutcome::Interrupted));
}

// List columns give size, checksum and time: such rows are registered without a HEAD, reads
// check the bytes against the list's checksum, and metadata hits carry the kept columns.
#[tokio::test]
async fn list_columns_recorded() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "files.csv" } },
        "rows": { "kind": "file", "source": "{url}", "target": "files/{name}",
                  "size": "bytes", "modified": "time",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }],
                  "keep": ["note"], "text": ["note"] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let good = format!("{:x}", md5::compute(b"sums"));
    let wrong = format!("{:x}", md5::compute(b"other"));
    let csv = format!(
        "url,name,bytes,md5,time,note\n\
         {COLI}md5checksums.txt,good.txt,4,{good},2025-11-17T17:38:21Z,coli sums\n\
         {HUMAN}README.txt,wrong.txt,6,{wrong},,human readme\n\
         {HUMAN}README.txt,bad.txt,six,{good},,broken size\n\
         {COLI}md5checksums.txt,drift.txt,5,{good},,size off\n\
         {COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz,timed.fna,,,2020-01-02T03:04:05Z,headed\n"
    );
    let ctx = fixture.context(30).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let body = (Bytes::from(csv), &BTreeMap::new());
    let lists = ("lists", &lists.unwrap());
    write_object(&ctx, &fixture.spec, lists, "files.csv", body)
        .await
        .ok()
        .unwrap();

    assert_eq!(fixture.run(31).await, (4, 1));
    assert_eq!(
        site.count(&Method::HEAD, &format!("{COLI}md5checksums.txt")),
        0
    );
    assert_eq!(site.count(&Method::HEAD, &format!("{HUMAN}README.txt")), 0);
    // A row without a size is headed, and keeps the time the list gives.
    let headed = format!("{COLI}GCF_000005845.2_ASM584v2_genomic.fna.gz");
    assert_eq!(site.count(&Method::HEAD, &headed), 1);
    let timed = fixture.observation("files/timed.fna").await;
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_577_934_245);
    assert_eq!((timed.content_length, timed.last_modified), (4, Some(at)));
    assert_eq!(fixture.read("files/good.txt").await.unwrap(), b"sums");
    assert!(fixture.read("files/wrong.txt").await.is_err());
    // A source whose size differs is refused; no successor drops the list's checksum.
    let bucket = fixture.spec.bucket.clone();
    let before = fixture.head(&bucket, "files/drift.txt").await.unwrap().0;
    assert!(fixture.read("files/drift.txt").await.is_err());
    let after = fixture.head(&bucket, "files/drift.txt").await.unwrap().0;
    assert_eq!(before, after);

    let hits = fixture.hits("sums").await;
    assert_eq!(hits.len(), 1);
    let kept = &hits[0].metadata;
    assert_eq!(kept.get("note").map(String::as_str), Some("coli sums"));
    assert!(kept.keys().all(|key| !key.starts_with("aruna-")));
    server.abort();
}

// Record rows are trusted as they are: the source sees no request, the references keep the
// row's ETag, type, time and metadata, and a row without a source writes an empty object.
#[tokio::test]
async fn record_rows_trusted() {
    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "records.tsv" } },
        "format": { "delimiter": "\t", "quote": null },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let md5 = format!("{:x}", md5::compute(b"sums"));
    let meta = r#"{"note":"coli sums","aruna-search-text":"note"}"#;
    let tsv = format!(
        "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n\
         coli/sums.txt\t{COLI}md5checksums.txt\t4\t2025-11-17T17:38:21Z\t\"e1\"\ttext/plain\t{md5}\t{meta}\n\
         coli/\t\t0\t\t\t\t\t{meta}\n"
    );
    let ctx = fixture.context(40).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let body = (Bytes::from(tsv), &BTreeMap::new());
    let lists = ("lists", &lists.unwrap());
    write_object(&ctx, &fixture.spec, lists, "records.tsv", body)
        .await
        .ok()
        .unwrap();

    // The empty row counts as imported, like the row with a source.
    assert_eq!(fixture.run(41).await, (2, 0));
    assert!(site.requests.lock().unwrap().is_empty());
    let observation = fixture.observation("coli/sums.txt").await;
    let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_763_401_101);
    assert_eq!(observation.content_length, 4);
    assert_eq!(observation.etag.as_deref(), Some("\"e1\""));
    assert_eq!(observation.content_type.as_deref(), Some("text/plain"));
    assert_eq!(observation.last_modified, Some(at));
    let bucket = fixture.spec.bucket.clone();
    let (_, folder) = fixture.head(&bucket, "coli/").await.unwrap();
    assert_eq!(folder["note"], "coli sums");
    // The empty object is the import's like the reference, not an upload.
    let job_id = Some(JobId::from_bytes([41; 16]).as_ulid());
    for key in ["coli/sums.txt", "coli/"] {
        let origin = fixture.origin(key).await;
        assert_eq!(origin, Some(VersionOrigin::Source { job_id }));
    }
    let mut found = fixture.search("sums").await;
    found.sort();
    assert_eq!(found, ["coli/", "coli/sums.txt"]);
    server.abort();
}

/// Rows per second of a trusted record import into a searched distributed bucket:
/// `ARUNA_BENCH_ROWS` source rows and `ARUNA_BENCH_EMPTY` empty rows, spread over
/// `ARUNA_BENCH_JOBS` tables imported at once. Run with `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark"]
async fn record_import_rate() {
    let count = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let (rows, empty) = (
        count("ARUNA_BENCH_ROWS", 100_000),
        count("ARUNA_BENCH_EMPTY", 0),
    );
    let jobs = count("ARUNA_BENCH_JOBS", 1).max(1);
    let background = count("ARUNA_BENCH_BACKGROUND", 1) > 0;
    // Contiguous key blocks per job, as a split export gives, instead of interleaved rows.
    let blocks = count("ARUNA_BENCH_BLOCKS", 0) > 0;
    let ((completed, failed, conflicts), elapsed, index) =
        import_records(rows, empty, (jobs, blocks), background).await;
    println!(
        "record import: {rows} rows, {empty} empty, {jobs} jobs, {completed} completed, \
         {failed} failed, {conflicts} conflicts, {elapsed:.1} s, {:.0} rows/s, \
         index caught up {index:.1} s later",
        (rows + empty) as f64 / elapsed
    );
}

// With the drain and the feed beside the import, the catch-up leaves no feed row unapplied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_import_settles() {
    let ((completed, failed, _), ..) = import_records(300, 0, (2, true), true).await;
    assert_eq!((completed, failed), (300, 0));
}

/// The search feed rows still waiting for the feed.
async fn feed_rows(fixture: &Fixture) -> Vec<(aruna_core::types::Key, aruna_core::types::Value)> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::SEARCH_FEED_KEYSPACE;
    let read = StorageEffect::Iter {
        key_space: SEARCH_FEED_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 1,
        txn_id: None,
    };
    match fixture
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        event => panic!("the feed must be readable: {event:?}"),
    }
}

// Empty rows of one job and record rows of another write one range in turns, not in conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_rows_alternate() {
    assert_eq!(
        import_records(400, 100, (2, false), false).await.0,
        (500, 0, 0)
    );
}

/// The table line of record row `row`: key `samn<row>/assembly.fna.gz`, size `row`.
fn record_line(row: usize, meta: &str) -> String {
    let md5 = format!("{:x}", md5::compute(row.to_le_bytes()));
    format!(
        "samn{row:08}/assembly.fna.gz\t{COLI}{row:08}.fna.gz\t{row}\t2025-11-17T17:38:21Z\t\
         \"{md5}\"\tapplication/gzip\t{md5}\t{meta}\n"
    )
}

// A record import of several full pages stopped after its first page resumes there, beside a
// second job with empty rows on the same bucket: every key once, counters and search right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_pages_resume() {
    use crate::distribution::index_feed::{FeedLimits, Feeder};
    use crate::distribution::key_index::{KeyQuery, RangeIndexes};
    use crate::staging::page::rows::MAX_PAGE_ENTRIES;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
    use std::ops::Bound;

    let (endpoint, site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "first.tsv" } },
        "format": { "delimiter": "\t", "quote": null, "nulls": [""] },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table).await;
    let header = "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n";
    let meta = r#"{"note":"sample"}"#;
    let rows = 2 * MAX_PAGE_ENTRIES + 500;
    let first: String = (0..rows).map(|row| record_line(row, meta)).collect();
    let second: String = (rows..rows + 300)
        .map(|row| record_line(row, meta))
        .chain((0..3).map(|row| format!("samn{row:08}/\t\t0\t\t\t\t\t{meta}\n")))
        .collect();
    let ctx = fixture.context(50).await;
    let (set_id, range_id) = searched_set(&ctx, &fixture.spec).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await
    .unwrap();
    let mut specs = Vec::new();
    for (key, rows) in [("first.tsv", first), ("second.tsv", second)] {
        let body = (Bytes::from(format!("{header}{rows}")), &BTreeMap::new());
        write_object(&ctx, &fixture.spec, ("lists", &lists), key, body)
            .await
            .ok()
            .unwrap();
        let mut spec = fixture.spec.clone();
        let import = spec.table.as_mut().unwrap();
        import.source = serde_json::from_value(serde_json::json!({
            "object": { "bucket": "lists", "key": key }
        }))
        .unwrap();
        specs.push(spec);
    }

    // The shutdown comes once the first page is committed, so the job stops after its next one.
    let (progress, stop) = (ctx.progress.clone(), ctx.shutdown.clone());
    let watch = async move {
        while progress.snapshot().current < MAX_PAGE_ENTRIES as u64 {
            progress.flush_requested().await;
        }
        stop.cancel();
    };
    let run = crate::jobs::staging::run_staging_job(&ctx, &specs[0]);
    let (outcome, ()) = tokio::join!(run, watch);
    assert!(matches!(outcome, JobRunOutcome::Interrupted));
    let resumed = JobContext {
        shutdown: CancellationToken::new(),
        ..ctx
    };
    let mut other = fixture.context_for(&specs[1], 51).await;
    other.range_turns = resumed.range_turns.clone();
    let (first, second) = tokio::join!(
        crate::jobs::staging::run_staging_job(&resumed, &specs[0]),
        crate::jobs::staging::run_staging_job(&other, &specs[1]),
    );
    let counts = |outcome| match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items,
            failed_items,
        }) => (completed_items, failed_items),
        JobRunOutcome::Failed(error) | JobRunOutcome::Deferred(error) => {
            panic!("the import stopped: {error:?}")
        }
        _ => panic!("the import did not finish"),
    };
    assert_eq!(counts(first), (rows as u64, 0));
    assert_eq!(counts(second), (303, 0));
    assert!(site.requests.lock().unwrap().is_empty());

    // One version per key: a page committed twice would add versions, not objects.
    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key(&fixture.spec.bucket).into(),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = fixture
        .driver
        .storage_handle
        .send_storage_effect(read)
        .await
    else {
        panic!("the bucket's counters must exist");
    };
    let counters = BucketCounters::from_bytes(&value).unwrap();
    let keys = rows as u64 + 303;
    assert_eq!((counters.objects, counters.versions), (keys, keys));
    let bytes = (0..rows + 300).map(|row| row as u64).sum::<u64>();
    assert_eq!(counters.referenced_bytes, bytes);
    for row in [
        0,
        MAX_PAGE_ENTRIES - 1,
        MAX_PAGE_ENTRIES,
        rows - 1,
        rows + 299,
    ] {
        let key = format!("samn{row:08}/assembly.fna.gz");
        assert_eq!(fixture.observation(&key).await.content_length, row as u64);
    }

    // The feed finds no index at first; the first index generation is built from the heads.
    let indexes = Arc::new(RangeIndexes::new(fixture._dir.path().join("range-indexes")));
    let mut feeder = Feeder::new(indexes.clone(), FeedLimits::default());
    while feeder.pass(&fixture.driver).await {}
    for row in [0, MAX_PAGE_ENTRIES, rows - 1, rows + 299] {
        let query = KeyQuery {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            names: Some(format!("samn{row:08}")),
            meta: None,
            limit: 10,
        };
        let found = indexes.search(set_id, range_id, 1, query).await.unwrap();
        let mut expected = vec![format!("samn{row:08}/assembly.fna.gz")];
        // The empty rows are the folders of the first rows.
        if row < 3 {
            expected.insert(0, format!("samn{row:08}/"));
        }
        assert_eq!(found.keys, expected);
    }
    server.abort();
}

/// Imports `rows` record and `empty` empty rows over `jobs` tables at once, in key blocks or
/// interleaved, with drain and search feed beside them when `background`. Returns completed and
/// failed rows, storage conflicts, the jobs' seconds and the search index's catch-up seconds.
async fn import_records(
    rows: usize,
    empty: usize,
    (jobs, blocks): (usize, bool),
    background: bool,
) -> ((u64, u64, u64), f64, f64) {
    let (endpoint, _site, server) = serve(Site::ncbi).await;
    let table = serde_json::json!({
        "source": { "object": { "bucket": "lists", "key": "records-0.tsv" } },
        "format": { "delimiter": "\t", "quote": null, "nulls": [""] },
        "rows": { "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
                  "size": "size", "modified": "modified",
                  "checksums": [{ "algorithm": "md5", "column": "md5" }] }
    });
    let fixture = Fixture::new(&endpoint, table.clone()).await;
    let meta = r#"{"note":"sample"}"#;
    let header = "key\tsource\tsize\tmodified\tetag\tcontent_type\tmd5\tmetadata\n";
    let mut tables = vec![String::from(header); jobs];
    for row in 0..rows {
        let table = match blocks {
            true => row * jobs / rows,
            false => row % jobs,
        };
        tables[table].push_str(&record_line(row, meta));
    }
    for row in 0..empty {
        tables[row % jobs].push_str(&format!("samn{row:08}/\t\t0\t\t\t\t\t{meta}\n"));
    }
    let ctx = fixture.context(40).await;
    searched_set(&ctx, &fixture.spec).await;
    let lists = drive(
        GetBucketOperation::new("lists".to_string()),
        &fixture.driver,
    )
    .await;
    let lists = ("lists", &lists.unwrap());
    let mut runs = Vec::new();
    for (index, tsv) in tables.into_iter().enumerate() {
        let key = format!("records-{index}.tsv");
        let body = (Bytes::from(tsv), &BTreeMap::new());
        write_object(&ctx, &fixture.spec, lists, &key, body)
            .await
            .ok()
            .unwrap();
        let mut spec = fixture.spec.clone();
        let mut import = table.clone();
        import["source"]["object"]["key"] = key.into();
        spec.table = Some(serde_json::from_value(import).unwrap());
        let mut job = fixture.context_for(&spec, 41 + index as u8).await;
        // Jobs of one node share their range turns.
        job.range_turns = ctx.range_turns.clone();
        runs.push((job, spec));
    }

    // The replication drain and the search feed run beside the import, as on a node, where each
    // committed page schedules them: here every page a job commits wakes both.
    let (pages, _) = tokio::sync::watch::channel(0u64);
    let pages = Arc::new(pages);
    let watchers: Vec<_> = (runs.iter())
        .map(|(job, _)| {
            let (progress, pages) = (job.progress.clone(), pages.clone());
            tokio::spawn(async move {
                loop {
                    progress.flush_requested().await;
                    pages.send_modify(|count| *count += 1);
                }
            })
        })
        .collect();
    let drain = fixture.driver.clone();
    let mut woken = pages.subscribe();
    let drain = tokio::spawn(async move {
        if !background {
            return;
        }
        loop {
            let drained = crate::replication::queue::process_blob_batch(&drain).await;
            if !drained.is_ok_and(|drained| drained.has_more_due) && woken.changed().await.is_err()
            {
                return;
            }
        }
    });
    let feed = fixture.driver.clone();
    let indexes = fixture._dir.path().join("range-indexes");
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let mut woken = pages.subscribe();
    let feed = tokio::spawn(async move {
        use crate::distribution::index_feed::{FeedLimits, Feeder};
        use crate::distribution::key_index::RangeIndexes;
        let mut feeder = Feeder::new(Arc::new(RangeIndexes::new(indexes)), FeedLimits::default());
        while background && !stopping.is_cancelled() {
            if !feeder.pass(&feed).await {
                tokio::select! {
                    _ = stopping.cancelled() => {}
                    _ = woken.changed() => {}
                }
            }
        }
        feeder
    });
    let storage = &fixture.driver.storage_handle;
    let conflicts = storage.snapshot_metrics().conflicts_total;
    let started = std::time::Instant::now();
    // Each job runs as its own task, as on a node.
    let runs = runs.into_iter().map(|(job, spec)| {
        tokio::spawn(async move { crate::jobs::staging::run_staging_job(&job, &spec).await })
    });
    let outcomes = futures_util::future::join_all(runs).await;
    let elapsed = started.elapsed().as_secs_f64();
    // The catch-up counts from the jobs' end, the feed's pass running then included.
    let ended = std::time::Instant::now();
    let conflicts = storage.snapshot_metrics().conflicts_total - conflicts;
    for watcher in watchers {
        watcher.abort();
    }
    drain.abort();
    stop.cancel();
    let mut feeder = feed.await.unwrap();
    server.abort();
    // The index catches up with what the jobs wrote.
    while background && feeder.pass(&fixture.driver).await {}
    let index = ended.elapsed().as_secs_f64();
    if background {
        assert!(
            feed_rows(&fixture).await.is_empty(),
            "the catch-up applied every feed row"
        );
    }
    let (mut completed, mut failed) = (0, 0);
    for outcome in outcomes {
        let (completed_items, failed_items) = match outcome.unwrap() {
            JobRunOutcome::Succeeded(JobResultPayload::Staging {
                completed_items,
                failed_items,
            }) => (completed_items, failed_items),
            JobRunOutcome::Failed(error) | JobRunOutcome::Deferred(error) => {
                panic!("the table import stopped: {error:?}")
            }
            _ => panic!("the table import did not finish"),
        };
        (completed, failed) = (completed + completed_items, failed + failed_items);
    }
    ((completed, failed, conflicts), elapsed, index)
}

/// Makes the job's bucket a searched distributed set whose upper range, holding keys from `m`,
/// this member coordinates, with a full key sample like a large bucket; returns set and range.
async fn searched_set(ctx: &JobContext, spec: &StagingJobSpec) -> (Ulid, Ulid) {
    use crate::distribution::test_record::{binding, searched, store_record};
    use crate::distribution::test_store::{begin, commit, install, range};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{KEY_SAMPLE_KEYSPACE, RANGE_COUNTER_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::storage::distribution::KeyRange;
    use aruna_core::structs::storage::distribution::accounting::{
        RangeCounters, SAMPLE_SIZE, SampleEntry, sample_count_key,
    };
    let handle = &ctx.driver.storage_handle;
    let (snapshot, bytes) = searched();
    let set_id = snapshot.set_id();
    store_record(handle, set_id, bytes).await;
    let mut bucket = pinned_bucket(ctx, spec).await.ok().unwrap();
    bucket.distribution = Some(binding(&snapshot, 2, 5));
    let range_id = snapshot.range_for("samn").unwrap().range_id;
    let ranges = [KeyRange {
        range_id,
        ..range(1, "m", None)
    }];
    install(handle, set_id, &ranges).await;
    let full = RangeCounters {
        sampled: i128::from(SAMPLE_SIZE),
        ..RangeCounters::default()
    };
    let largest = SampleEntry {
        priority: u64::MAX,
        key: "largest".to_string(),
    };
    let txn_id = begin(handle).await;
    let effects = [
        StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: spec.bucket.as_bytes().to_vec().into(),
            value: bucket.to_bytes().unwrap().into(),
            txn_id: Some(txn_id),
        },
        StorageEffect::AddCounters {
            key_space: RANGE_COUNTER_KEYSPACE.to_string(),
            deltas: vec![(sample_count_key(set_id, range_id).into(), full)],
            txn_id,
        },
        StorageEffect::Write {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            key: largest.row_key(set_id, range_id).into(),
            value: Vec::new().into(),
            txn_id: Some(txn_id),
        },
    ];
    for effect in effects {
        handle.send_storage_effect(effect).await;
    }
    assert!(commit(handle, txn_id).await);
    (set_id, range_id)
}
