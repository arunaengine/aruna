mod shared;

use reqwest::StatusCode;
use reqwest::header;
use shared::{TEST_CORS_ORIGIN, TestResult, spawn_full_seed_node, spawn_seed_node};

const DISALLOWED_ORIGIN: &str = "http://evil.test";

#[tokio::test]
async fn rest_preflight_and_actual_requests_carry_cors_headers() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let client = reqwest::Client::new();

    let preflight = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/api/v1/info", seed.base_url),
        )
        .header(header::ORIGIN, TEST_CORS_ORIGIN)
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization")
        .send()
        .await?;
    assert!(preflight.status().is_success());
    assert_eq!(
        preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some(TEST_CORS_ORIGIN)
    );
    let allow_headers = preflight
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(allow_headers.contains("authorization"));

    let actual = client
        .get(format!("{}/api/v1/info", seed.base_url))
        .header(header::ORIGIN, TEST_CORS_ORIGIN)
        .send()
        .await?;
    assert!(actual.status().is_success());
    assert_eq!(
        actual
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some(TEST_CORS_ORIGIN)
    );

    let denied = client
        .get(format!("{}/api/v1/info", seed.base_url))
        .header(header::ORIGIN, DISALLOWED_ORIGIN)
        .send()
        .await?;
    assert!(
        denied
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn s3_preflight_succeeds_unsigned_and_responses_expose_headers() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let s3 = seed.s3.as_ref().expect("full seed node has S3");
    let client = reqwest::Client::new();

    // Unsigned preflight must be answered before signature validation.
    let preflight = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/some-bucket/some/key", s3.endpoint_url),
        )
        .header(header::ORIGIN, TEST_CORS_ORIGIN)
        .header("access-control-request-method", "PUT")
        .header("access-control-request-headers", "authorization,x-amz-date")
        .send()
        .await?;
    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some(TEST_CORS_ORIGIN)
    );
    let allow_methods = preflight
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_METHODS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(allow_methods.contains("PUT"));
    assert_eq!(
        preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .and_then(|value| value.to_str().ok()),
        Some("authorization,x-amz-date")
    );

    // Preflight from a disallowed origin completes without CORS headers.
    let denied = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/some-bucket/some/key", s3.endpoint_url),
        )
        .header(header::ORIGIN, DISALLOWED_ORIGIN)
        .header("access-control-request-method", "PUT")
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::NO_CONTENT);
    assert!(
        denied
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );

    // Real (unsigned, hence rejected) requests still carry CORS headers so the
    // browser can read the error response.
    let unauthorized = client
        .get(format!("{}/some-bucket/some/key", s3.endpoint_url))
        .header(header::ORIGIN, TEST_CORS_ORIGIN)
        .send()
        .await?;
    assert!(!unauthorized.status().is_success());
    assert_eq!(
        unauthorized
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some(TEST_CORS_ORIGIN)
    );
    let exposed = unauthorized
        .headers()
        .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(exposed.contains("etag"));

    seed.shutdown().await;
    Ok(())
}
