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
        let (job_id, claim_token) = (JobId::from_bytes([seed; 16]), Ulid::from_bytes([seed; 16]));
        let owner = self.spec.auth_context.user_id;
        let payload = JobPayload::Staging(self.spec.clone());
        let mut record = JobRecord::new(job_id, payload, owner, self.spec.node_id, 1, 1, None);
        record.claim = Some(JobClaim {
            holder_node_id: self.spec.node_id,
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
        page.hits.into_iter().map(|hit| hit.hit.key).collect()
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
