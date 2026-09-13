//! Protocol responses and CORS answers of the S3 listener.
//!
//! These builders decide status codes, headers and bodies for requests that
//! never reach the s3s service: rate limits, oversized bodies, invalid buckets,
//! timeouts and CORS preflight. Keeping them here keeps those S3-specific
//! shapes out of the shared REST/MCP layers.

use crate::cors::CorsConfig;
use crate::s3::cors::{
    build_preflight_response, forbidden_preflight_response, inject_cors_headers, match_actual_rule,
    match_preflight_rule,
};
use crate::s3::server::classification::RequestClassification;
use aruna_core::structs::BucketCorsConfiguration;
use http::header;
use http::{Method, StatusCode};
use s3s::HttpError;
use s3s::HttpResponse;
use s3s::s3_error;
use std::io;

pub(super) fn connection_error() -> HttpError {
    HttpError::new(Box::new(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "S3 connection became idle",
    )))
}

pub(super) fn stream_timeout_response() -> Result<HttpResponse, HttpError> {
    s3_error!(RequestTimeout, "S3 request made no progress")
        .to_http_response()
        .map_err(|error| HttpError::new(Box::new(error)))
}

pub(super) fn oversized_delete_response() -> Result<HttpResponse, HttpError> {
    s3_error!(
        MaxMessageLengthExceeded,
        "DeleteObjects request body exceeds 2 MiB"
    )
    .to_http_response()
    .map_err(|error| HttpError::new(Box::new(error)))
}

pub(super) fn invalid_bucket_response(reason: &'static str) -> Result<HttpResponse, HttpError> {
    s3_error!(InvalidBucketName, "{}", reason)
        .to_http_response()
        .map_err(|error| HttpError::new(Box::new(error)))
}

pub(super) fn slow_down_response(retry_after: u64) -> HttpResponse {
    const SLOW_DOWN_BODY: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>";
    let mut response = http::Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(s3s::Body::from(SLOW_DOWN_BODY.to_vec()))
        .expect("static response must build");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/xml"),
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, header::HeaderValue::from(retry_after));
    response
}

/// Answers a CORS preflight before s3s signature validation: an unsigned
/// OPTIONS request must not fail with 403. Bucket rules extend the node
/// allowlist, which remains authoritative for portals.
pub(super) fn preflight_response(
    classification: &RequestClassification,
    bucket_cors: Option<&BucketCorsConfiguration>,
    cors: &CorsConfig,
) -> Option<HttpResponse> {
    if classification.method != Method::OPTIONS {
        return None;
    }
    let origin_header = classification.origin_header.as_ref()?;
    let bucket_rule = bucket_cors.and_then(|config| {
        classification
            .requested_method
            .as_deref()
            .and_then(|requested_method| {
                match_preflight_rule(
                    config,
                    classification.origin.as_deref().unwrap_or_default(),
                    requested_method,
                    &classification.requested_headers,
                )
            })
    });

    let response = if let Some(matched_rule) = bucket_rule {
        build_preflight_response(matched_rule)
    } else if let Some(cors_headers) = cors.s3_preflight_headers(
        origin_header,
        classification.requested_headers_value.as_ref(),
    ) {
        let mut response = http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(s3s::Body::empty())
            .expect("static response must build");
        response.headers_mut().extend(cors_headers);
        response
    } else if bucket_cors.is_some() {
        forbidden_preflight_response()
    } else {
        http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(s3s::Body::empty())
            .expect("static response must build")
    };
    Some(response)
}

/// Injects the bucket rule's CORS headers into a normal response, falling back
/// to the node allowlist so allowlisted origins remain readable when bucket
/// rules omit them.
pub(super) fn apply_response_cors(
    response: &mut HttpResponse,
    classification: &RequestClassification,
    bucket_cors: Option<&BucketCorsConfiguration>,
    cors: &CorsConfig,
) {
    let bucket_rule = bucket_cors.and_then(|config| {
        classification
            .origin
            .as_deref()
            .and_then(|origin| match_actual_rule(config, origin, &classification.method))
    });
    if let Some(matched_rule) = bucket_rule {
        inject_cors_headers(response, matched_rule);
    } else {
        cors.apply_s3_headers(
            classification.origin_header.as_ref(),
            response.headers_mut(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::server::classification::RequestClassification;
    use aruna_core::structs::BucketCorsRule;
    use http::HeaderValue;

    fn classification(
        method: Method,
        origin: Option<&str>,
        requested: Option<&str>,
    ) -> RequestClassification {
        let mut headers = http::HeaderMap::new();
        if let Some(origin) = origin {
            headers.insert(
                header::ORIGIN,
                HeaderValue::try_from(origin).expect("origin"),
            );
        }
        if let Some(requested) = requested {
            headers.insert(
                header::ACCESS_CONTROL_REQUEST_METHOD,
                HeaderValue::try_from(requested).expect("requested method"),
            );
        }
        let mut request = http::Request::builder()
            .method(method)
            .uri("https://s3.example/bucket/key")
            .body(())
            .expect("request parts build");
        *request.headers_mut() = headers;
        RequestClassification::classify(&request.into_parts().0, "s3.example")
    }

    fn bucket_config() -> BucketCorsConfiguration {
        BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: Some("portal".to_string()),
                allowed_origins: vec!["https://portal.test".to_string()],
                allowed_methods: vec!["PUT".to_string()],
                allowed_headers: vec!["content-type".to_string()],
                expose_headers: vec!["etag".to_string()],
                max_age_seconds: Some(600),
            }],
        }
    }

    #[test]
    fn preflight_matches_bucket_rule() {
        let classification =
            classification(Method::OPTIONS, Some("https://portal.test"), Some("PUT"));
        let response = preflight_response(
            &classification,
            Some(&bucket_config()),
            &CorsConfig::default(),
        )
        .expect("preflight answers");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://portal.test"
        );
        assert!(
            response
                .headers()
                .contains_key(header::ACCESS_CONTROL_MAX_AGE)
        );
    }

    #[test]
    fn preflight_bucket_refusal_is_forbidden() {
        let classification =
            classification(Method::OPTIONS, Some("https://elsewhere.test"), Some("PUT"));
        let response = preflight_response(
            &classification,
            Some(&bucket_config()),
            &CorsConfig::default(),
        )
        .expect("preflight answers");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn preflight_falls_back_to_node_allowlist() {
        let classification =
            classification(Method::OPTIONS, Some("https://portal.test"), Some("PUT"));
        let cors = CorsConfig::new(vec!["https://portal.test".to_string()]);
        let response = preflight_response(&classification, None, &cors).expect("preflight answers");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://portal.test"
        );
    }

    #[test]
    fn non_preflight_stays_untouched() {
        let classification = classification(Method::PUT, Some("https://portal.test"), None);
        assert!(
            preflight_response(
                &classification,
                Some(&bucket_config()),
                &CorsConfig::default()
            )
            .is_none()
        );
    }

    #[test]
    fn actual_response_uses_bucket_rule() {
        let classification = classification(Method::PUT, Some("https://portal.test"), None);
        let mut response = http::Response::new(s3s::Body::empty());
        apply_response_cors(
            &mut response,
            &classification,
            Some(&bucket_config()),
            &CorsConfig::default(),
        );
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://portal.test"
        );
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS],
            "etag"
        );
    }

    #[test]
    fn slows_full_request() {
        let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("first permit");
        let response = match limit.clone().try_acquire_owned() {
            Ok(_) => panic!("request unexpectedly admitted"),
            Err(tokio::sync::TryAcquireError::NoPermits | tokio::sync::TryAcquireError::Closed) => {
                slow_down_response(1)
            }
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        drop(permit);
    }
}
