mod shared;

use reqwest::StatusCode;
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_s3_credentials_via_http,
    s3_client, spawn_full_seed_node, spawn_seed_node, wait_for_group_via_http,
};

async fn scrape(ops_url: &str) -> TestResult<String> {
    let response = reqwest::get(format!("{ops_url}/metrics")).await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(response.text().await?)
}

fn gauge_value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
}

#[tokio::test]
async fn readyz_reflects_startup_gate() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        let client = reqwest::Client::new();

        let health = client
            .get(format!("{}/healthz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(health.text().await?, "ok");

        let before = client
            .get(format!("{}/readyz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(before.status(), StatusCode::SERVICE_UNAVAILABLE);
        let before_body: serde_json::Value = before.json().await?;
        assert_eq!(before_body["ready"], serde_json::json!(false));
        assert!(
            before_body["checks"]["startup"]
                .as_str()
                .unwrap_or_default()
                .starts_with("failed"),
            "unexpected startup check: {before_body}"
        );

        seed.readiness.set_ready();

        let after = client
            .get(format!("{}/readyz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(after.status(), StatusCode::OK, "node should be ready");
        let after_body: serde_json::Value = after.json().await?;
        assert_eq!(after_body["ready"], serde_json::json!(true));

        // Liveness is unconditional.
        let health = client
            .get(format!("{}/healthz", seed.ops_url))
            .send()
            .await?;
        assert_eq!(health.status(), StatusCode::OK);
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metrics_expose_rest_storage_and_queue_series() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let result = async {
        let client = reqwest::Client::new();
        // Drive one REST request so the counter has a rest sample.
        client
            .get(format!("{}/api/v1/info", seed.base_url))
            .send()
            .await?;

        let body = scrape(&seed.ops_url).await?;
        assert!(
            body.contains("aruna_http_requests_total"),
            "missing request counter: {body}"
        );
        assert!(
            body.contains("interface=\"rest\""),
            "missing rest interface sample: {body}"
        );
        assert!(
            body.contains("aruna_storage_requests "),
            "missing storage gauge: {body}"
        );
        assert!(
            gauge_value(&body, "aruna_queue_depth{queue=\"document_sync_outbox\"}")
                .is_some_and(|depth| depth >= 0.0),
            "outbox depth gauge missing or negative: {body}"
        );
        assert!(
            gauge_value(
                &body,
                "aruna_queue_probe_up{queue=\"document_sync_outbox\"}"
            )
            .is_some(),
            "outbox probe health gauge missing: {body}"
        );
        assert!(
            gauge_value(&body, "aruna_node_started").is_some(),
            "node_started gauge missing: {body}"
        );
        assert!(
            body.contains("aruna_build_info{version=\""),
            "missing build info: {body}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn metrics_expose_s3_operation_label() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let result = async {
        let bearer_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group =
            create_group_via_http(&seed.base_url, &bearer_token, "obs-metrics-group").await?;
        wait_for_group_via_http(&seed.base_url, &bearer_token, &group.group_id).await?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer_token, &group.group_id).await?;

        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let s3 = s3_client(endpoint, &credentials);
        s3.create_bucket()
            .bucket("obs-metrics-bucket")
            .send()
            .await?;

        let body = scrape(&seed.ops_url).await?;
        assert!(
            body.contains("interface=\"s3\""),
            "missing s3 interface sample: {body}"
        );
        assert!(
            body.contains("op=\"CreateBucket\""),
            "missing resolved CreateBucket op label: {body}"
        );
        assert!(
            gauge_value(
                &body,
                "aruna_http_request_duration_seconds_count{interface=\"s3\",op=\"CreateBucket\"}",
            )
            .is_some_and(|count| count >= 1.0),
            "CreateBucket duration count missing: {body}"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    seed.shutdown().await;
    result
}
