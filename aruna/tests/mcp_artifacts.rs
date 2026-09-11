// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]
//! End-to-end artifact contract over MCP: a script run captures a PNG, the
//! compute tools report it with its content type and exact version, and the S3
//! surface serves those bytes. Skips with a message when no Docker daemon is
//! reachable.
#![cfg(feature = "docker")]

mod shared;

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_compute::executor::docker::DockerBackend;
use aruna_compute::{DockerConfig, ExecutorBackend, ExecutorRegistry};
use aruna_core::structs::JobId;
use aws_sdk_s3::primitives::ByteStream;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientInfo, ProtocolVersion};
use rmcp::service::RunningService;
use rmcp::transport::{
    StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
};
use rmcp::{ClientLifecycleMode, ClientServiceExt, RoleClient};
use serde_json::{Value, json};
use shared::{
    S3Credentials, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, s3_client, spawn_compute_seed, wait_for_group_via_http,
};
use ulid::Ulid;

const CHART_SCRIPT: &str = r#"import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.bar(["a", "b", "c"], [3, 1, 2])
plt.savefig("/work/chart.png")
"#;

/// A reachable, healthy Docker daemon, or `None` (test skips).
async fn docker_or_skip() -> Option<DockerBackend> {
    let config = DockerConfig {
        keep_failed: std::env::var("ARUNA_KEEP_FAILED").is_ok(),
        ..DockerConfig::default()
    };
    match DockerBackend::with_config(config) {
        Ok(backend) => match backend.health().await {
            Ok(()) => Some(backend),
            Err(error) => {
                eprintln!("skipping artifact test: docker daemon unhealthy: {error}");
                None
            }
        },
        Err(error) => {
            eprintln!("skipping artifact test: no reachable docker daemon: {error}");
            None
        }
    }
}

struct Fixture {
    seed: shared::SeedNode,
    group_id: Ulid,
    bearer: String,
    s3: S3Credentials,
    endpoint: shared::S3Endpoint,
    bucket: String,
}

/// Docker-backed node running its own jobs + a group bucket holding a few dated
/// objects the aggregation counts.
async fn setup(backend: DockerBackend) -> TestResult<Fixture> {
    let registry = Arc::new(ExecutorRegistry::new().with_backend(Arc::new(backend)));
    let seed = spawn_compute_seed(registry).await?;
    let endpoint = seed.s3.clone().expect("full node exposes S3");
    let bearer = create_bearer_token(
        &seed.context,
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &bearer, "mcp-artifacts").await?;
    wait_for_group_via_http(&seed.base_url, &bearer, &group.group_id).await?;
    let group_id = Ulid::from_string(&group.group_id)?;
    let creds = create_s3_credentials_via_http(&seed.base_url, &bearer, &group.group_id).await?;

    let bucket = format!("work-{}", Ulid::generate().to_string().to_lowercase());
    let client = s3_client(&endpoint, &creds);
    client.create_bucket().bucket(&bucket).send().await?;
    for index in 0..3 {
        client
            .put_object()
            .bucket(&bucket)
            .key(format!("docs/note-{index}.txt"))
            .body(ByteStream::from(format!("note {index}\n").into_bytes()))
            .send()
            .await?;
    }

    Ok(Fixture {
        seed,
        group_id,
        bearer,
        s3: creds,
        endpoint,
        bucket,
    })
}

async fn connect(base_url: &str, token: &str) -> RunningService<RoleClient, ClientInfo> {
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("{base_url}/mcp"))
            .auth_header(token.to_string()),
    );
    ClientInfo::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("MCP client connects to the node")
}

async fn call(client: &RunningService<RoleClient, ClientInfo>, tool: &str, args: Value) -> Value {
    let request = match args {
        Value::Null => CallToolRequestParams::new(tool.to_string()),
        value => CallToolRequestParams::new(tool.to_string())
            .with_arguments(value.as_object().expect("tool arguments object").clone()),
    };
    let result = client
        .call_tool(request)
        .await
        .unwrap_or_else(|error| panic!("{tool} call failed: {error}"));
    assert!(!is_error(&result), "{tool} refused the call: {result:?}");
    result
        .structured_content
        .unwrap_or_else(|| panic!("{tool} returned no structured content"))
}

fn is_error(result: &CallToolResult) -> bool {
    result.is_error == Some(true)
}

/// One `get_job` answer, or the refusal while the family cannot be served yet.
async fn job_view(
    client: &RunningService<RoleClient, ClientInfo>,
    job_id: JobId,
) -> Result<Value, String> {
    let request = CallToolRequestParams::new("get_job".to_string()).with_arguments(
        json!({ "id": job_id.to_string() })
            .as_object()
            .expect("tool arguments object")
            .clone(),
    );
    let result = client
        .call_tool(request)
        .await
        .map_err(|error| error.to_string())?;
    match result.structured_content {
        Some(content) if !is_error(&result) => Ok(content),
        other => Err(format!("{other:?}")),
    }
}

/// Waits on a lost-progress window rather than a wall-clock budget: an image
/// pull outlasts any fixed budget while the view still moves. A family whose
/// records are still landing answers unavailable, so a refusal is retried.
async fn wait_view(
    client: &RunningService<RoleClient, ClientInfo>,
    job_id: JobId,
    idle: Duration,
) -> Value {
    let mut progress = None;
    let mut deadline = Instant::now() + idle;
    let mut last = Err("get_job was never answered".to_string());
    loop {
        last = job_view(client, job_id).await.or(last);
        let state = last
            .as_ref()
            .ok()
            .and_then(|job| job["state"].as_str())
            .map(str::to_string);
        if matches!(state.as_deref(), Some("succeeded" | "failed" | "cancelled")) {
            return last.expect("terminal view");
        }
        if state.is_some() && state != progress {
            progress = state;
            deadline = Instant::now() + idle;
        }
        if Instant::now() >= deadline {
            return last.unwrap_or_else(|error| panic!("get_job stayed unavailable: {error}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// A chart written by a script run is discoverable and displayable: the tools
// name its type, size, filename, and exact version, and S3 serves those bytes.
#[tokio::test]
async fn artifact_round_trip() -> TestResult<()> {
    let Some(backend) = docker_or_skip().await else {
        return Ok(());
    };
    let fixture = setup(backend).await?;
    let client = connect(&fixture.seed.base_url, &fixture.bearer).await;

    let listed = call(
        &client,
        "list_objects",
        json!({ "bucket": fixture.bucket, "prefix": "docs/" }),
    )
    .await;
    assert_eq!(
        listed["objects"].as_array().map(Vec::len),
        Some(3),
        "the seeded objects are listed"
    );

    let aggregated = call(
        &client,
        "aggregate_objects",
        json!({ "bucket": fixture.bucket, "prefix": "docs/", "bucket_by": "week" }),
    )
    .await;
    assert_eq!(aggregated["total_count"], json!(3));
    assert_eq!(aggregated["truncated"], json!(false));
    assert_eq!(
        aggregated["buckets"].as_array().map(Vec::len),
        Some(1),
        "objects written together share one week"
    );
    assert_eq!(aggregated["buckets"][0]["count"], json!(3));

    let submitted = call(
        &client,
        "run_script",
        json!({
            "group_id": fixture.group_id.to_string(),
            "bucket": fixture.bucket,
            "runtime": "python-uv",
            "script": CHART_SCRIPT,
            "dependencies": ["matplotlib"],
            "outputs": [{ "container_path": "/work/chart.png", "dest_key": "results/chart.png" }],
            "max_walltime_ms": 600000
        }),
    )
    .await;
    let job_id: JobId = submitted["job_id"]
        .as_str()
        .expect("run_script returns a job id")
        .parse()?;

    let job = wait_view(&client, job_id, Duration::from_secs(300)).await;
    assert_eq!(job["state"], "succeeded", "the chart run must succeed");

    let outputs = call(
        &client,
        "list_job_outputs",
        json!({ "id": job_id.to_string() }),
    )
    .await;
    let entries = outputs["outputs"].as_array().expect("outputs array");
    assert_eq!(entries.len(), 1, "one declared output was captured");
    let artifact = &entries[0];
    assert_eq!(artifact["content_type"], "image/png");
    assert_eq!(artifact["filename"], "chart.png");
    assert_eq!(artifact["key"], "results/chart.png");
    assert_eq!(artifact["bucket"], json!(fixture.bucket));
    assert!(
        artifact["size"].as_u64().unwrap_or_default() > 0,
        "the captured chart has bytes"
    );
    let version_id = artifact["version_id"]
        .as_str()
        .expect("the capture names its version")
        .to_string();

    let stat = call(
        &client,
        "stat_object",
        json!({
            "bucket": fixture.bucket,
            "key": "results/chart.png",
            "version_id": version_id
        }),
    )
    .await;
    assert_eq!(stat["version_id"], json!(version_id));
    assert_eq!(stat["content_type"], "image/png");
    assert_eq!(stat["size"], artifact["size"]);

    let s3 = s3_client(&fixture.endpoint, &fixture.s3);
    let object = s3
        .get_object()
        .bucket(&fixture.bucket)
        .key("results/chart.png")
        .version_id(&version_id)
        .send()
        .await?;
    assert_eq!(object.content_type(), Some("image/png"));
    let bytes = object.body.collect().await?.into_bytes();
    assert_eq!(
        &bytes[..8],
        b"\x89PNG\r\n\x1a\n".as_slice(),
        "the served bytes are a PNG"
    );

    client.cancel().await.ok();
    fixture.seed.shutdown().await;
    Ok(())
}
