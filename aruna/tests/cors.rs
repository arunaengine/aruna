mod shared;

use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use reqwest::StatusCode;
use reqwest::header;
use shared::{
    TEST_CORS_ORIGIN, TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_via_http, s3_client, spawn_full_seed_node, spawn_seed_node,
};

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

#[tokio::test]
async fn s3_bucket_cors_rules_never_lock_out_allowlisted_origins() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;

    let result = async {
        let bearer = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_via_http(&seed.base_url, &bearer, "cors-locked").await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer, &group.group_id).await?;
        let s3 = s3_client(endpoint, &credentials);

        let bucket = "cors-locked-bucket";
        let object_url = format!("{}/{}/profiles/shapes.ttl", endpoint.endpoint_url, bucket);
        s3.create_bucket().bucket(bucket).send().await?;
        // A stored read-only rule: the shape older portal versions wrote,
        // which used to block every browser write on the bucket - including
        // the PutBucketCors call that would repair it.
        s3.put_bucket_cors()
            .bucket(bucket)
            .cors_configuration(
                CorsConfiguration::builder()
                    .cors_rules(
                        CorsRule::builder()
                            .allowed_methods("GET")
                            .allowed_methods("HEAD")
                            .allowed_origins("*")
                            .allowed_headers("*")
                            .build()?,
                    )
                    .build()?,
            )
            .send()
            .await?;

        let client = reqwest::Client::new();

        // What the stored rule covers keeps working: public GET preflight
        // from an arbitrary origin.
        let public_read = client
            .request(reqwest::Method::OPTIONS, &object_url)
            .header(header::ORIGIN, DISALLOWED_ORIGIN)
            .header("access-control-request-method", "GET")
            .send()
            .await?;
        assert_eq!(public_read.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            public_read
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );

        // A PUT preflight from the node's allowlisted origin is not covered
        // by the stored rule; it must fall back to the node allowlist instead
        // of answering 403.
        let portal_put = client
            .request(reqwest::Method::OPTIONS, &object_url)
            .header(header::ORIGIN, TEST_CORS_ORIGIN)
            .header("access-control-request-method", "PUT")
            .header("access-control-request-headers", "authorization,x-amz-date")
            .send()
            .await?;
        assert_eq!(portal_put.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            portal_put
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some(TEST_CORS_ORIGIN)
        );
        assert!(
            portal_put
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .contains("PUT")
        );

        // A foreign origin matches neither the stored rule nor the allowlist.
        let foreign_put = client
            .request(reqwest::Method::OPTIONS, &object_url)
            .header(header::ORIGIN, DISALLOWED_ORIGIN)
            .header("access-control-request-method", "PUT")
            .send()
            .await?;
        assert_eq!(foreign_put.status(), StatusCode::FORBIDDEN);
        assert!(
            foreign_put
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );

        // Covered actual requests still answer from the stored rule.
        let covered_get = client
            .get(&object_url)
            .header(header::ORIGIN, TEST_CORS_ORIGIN)
            .send()
            .await?;
        assert_eq!(
            covered_get
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );

        // Actual responses the stored rule does NOT cover fall back too, so
        // the allowlisted origin can read error bodies (here: the signature
        // rejection of an unsigned PUT) instead of an opaque CORS failure.
        let uncovered_put = client
            .put(&object_url)
            .header(header::ORIGIN, TEST_CORS_ORIGIN)
            .body("x")
            .send()
            .await?;
        assert!(!uncovered_put.status().is_success());
        assert_eq!(
            uncovered_put
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some(TEST_CORS_ORIGIN)
        );

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
