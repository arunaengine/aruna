//! Request classification for the S3 listener: bucket, CORS preflight inputs,
//! DeleteObjects body shape, and lane assignment in one synchronous, I/O-free
//! pass before anything may parse or store a request.

use crate::s3::cors::parse_requested_headers;
use crate::s3::server::body::DELETE_OBJECTS_MAX_BODY;
use crate::s3::util::bucket_name_reason;
use http::{HeaderMap, HeaderValue, Method, header};
use s3s::host::S3Host;
use std::net::{IpAddr, SocketAddr};

/// One request's route, CORS and body classification.
pub(super) struct RequestClassification {
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) bucket: Option<String>,
    pub(super) invalid_bucket: Option<&'static str>,
    pub(super) origin_header: Option<HeaderValue>,
    pub(super) origin: Option<String>,
    pub(super) requested_method: Option<String>,
    pub(super) requested_headers_value: Option<HeaderValue>,
    pub(super) requested_headers: Vec<String>,
    pub(super) delete_objects: bool,
    pub(super) oversized_delete: bool,
    pub(super) complete_multipart: bool,
    pub(super) bulk_request: bool,
}

impl RequestClassification {
    pub(super) fn classify(parts: &http::request::Parts, domain: &str) -> Self {
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        let host = parts
            .headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok());
        let bucket = extract_bucket_name(host, &path, domain);
        // s3s rejects a malformed bucket at path parse without a message, which
        // reaches clients as "UnknownError"; answer the violated rule instead.
        let invalid_bucket = bucket.as_deref().and_then(bucket_name_reason);
        let origin_header = parts.headers.get(header::ORIGIN).cloned();
        let origin = origin_header
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let requested_method = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let requested_headers_value = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .cloned();
        let requested_headers = requested_headers_value
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .map(parse_requested_headers)
            .unwrap_or_default();
        let delete_objects = method == Method::POST
            && parts.uri.query().is_some_and(|query| {
                url::form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == "delete")
            });
        let oversized_delete = delete_objects
            && parts
                .headers
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > DELETE_OBJECTS_MAX_BODY as u64);
        // A completion is the one S3 call that can legitimately run for minutes,
        // and its client sends no bytes while it waits.
        let complete_multipart = method == Method::POST && query_has_any(&parts.uri, &["uploadId"]);
        let bulk_request =
            is_bulk_request(&method, host, &path, domain, &parts.uri, &parts.headers);

        Self {
            method,
            path,
            bucket,
            invalid_bucket,
            origin_header,
            origin,
            requested_method,
            requested_headers_value,
            requested_headers,
            delete_objects,
            oversized_delete,
            complete_multipart,
            bulk_request,
        }
    }
}

fn query_has_any(uri: &http::Uri, names: &[&str]) -> bool {
    uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .any(|(key, _)| names.iter().any(|name| key.as_ref() == *name))
    })
}

fn query_value(uri: &http::Uri, name: &str, expected: &str) -> bool {
    uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .any(|(key, value)| key.as_ref() == name && value.as_ref() == expected)
    })
}

fn request_path(host: Option<&str>, path: &str, domain: &str) -> Option<s3s::path::S3Path> {
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .ok()?;
    if let Some(host) = host
        && host.parse::<SocketAddr>().is_err()
        && host.parse::<IpAddr>().is_err()
    {
        let s3_host = s3s::host::SingleDomain::new(domain).ok()?;
        let virtual_host = s3_host.parse_host_header(host).ok()?;
        return s3s::path::parse_virtual_hosted_style(virtual_host.bucket(), path.as_ref()).ok();
    }

    s3s::path::parse_path_style(path.as_ref()).ok()
}

fn is_multipart(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("multipart/form-data"))
}

fn is_bulk_request(
    method: &Method,
    host: Option<&str>,
    path: &str,
    domain: &str,
    uri: &http::Uri,
    headers: &HeaderMap,
) -> bool {
    // s3s resolves the operation later, so admission mirrors only data-heavy routes.
    let parsed_path = request_path(host, path, domain);
    let object = parsed_path
        .as_ref()
        .is_some_and(|path| path.as_object().is_some_and(|(_, key)| !key.is_empty()));
    let bucket = parsed_path
        .as_ref()
        .is_some_and(|path| path.as_bucket().is_some());
    let root = parsed_path.as_ref().is_some_and(s3s::path::S3Path::is_root);
    match method.as_str() {
        "GET" => {
            root || (bucket
                && !query_has_any(
                    uri,
                    &[
                        "analytics",
                        "intelligent-tiering",
                        "inventory",
                        "metrics",
                        "session",
                        "accelerate",
                        "acl",
                        "cors",
                        "encryption",
                        "lifecycle",
                        "location",
                        "logging",
                        "metadataTable",
                        "notification",
                        "ownershipControls",
                        "policy",
                        "policyStatus",
                        "replication",
                        "requestPayment",
                        "tagging",
                        "versioning",
                        "website",
                        "object-lock",
                        "publicAccessBlock",
                    ],
                ))
                || (object
                    && !query_has_any(
                        uri,
                        &[
                            "attributes",
                            "acl",
                            "legal-hold",
                            "retention",
                            "tagging",
                            "torrent",
                            "uploadId",
                        ],
                    ))
        }
        "PUT" => object && !query_has_any(uri, &["acl", "legal-hold", "retention", "tagging"]),
        "DELETE" => object && !query_has_any(uri, &["tagging"]),
        "POST" => {
            let select =
                object && query_has_any(uri, &["select"]) && query_value(uri, "select-type", "2");
            let control = if select {
                false
            } else if object {
                query_has_any(uri, &["uploads", "restore"])
            } else {
                query_has_any(uri, &["metadataTable"])
            };
            !control
                && (select
                    || (object && query_has_any(uri, &["uploadId"]))
                    || (bucket && query_has_any(uri, &["delete"]))
                    || ((object || bucket) && is_multipart(headers)))
        }
        _ => false,
    }
}

fn extract_bucket_name(host: Option<&str>, path: &str, domain: &str) -> Option<String> {
    if let Some(host) = host
        && let Some(bucket) = virtual_hosted_bucket(host, domain)
    {
        return Some(bucket);
    }

    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_owned)
}

fn virtual_hosted_bucket(host: &str, domain: &str) -> Option<String> {
    let host = host.split(':').next().unwrap_or(host);
    let domain = domain.split(':').next().unwrap_or(domain);
    let prefix = host.strip_suffix(domain)?.strip_suffix('.')?;
    (!prefix.is_empty()).then(|| prefix.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(method: Method, uri: &str, headers: HeaderMap) -> http::request::Parts {
        let mut request = http::Request::builder()
            .method(method)
            .uri(uri)
            .body(())
            .expect("request parts build");
        *request.headers_mut() = headers;
        request.into_parts().0
    }

    #[test]
    fn classifies_bulk_routes() {
        let headers = HeaderMap::new();
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/",
            "s3.example",
            &http::Uri::from_static("/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
        for query in ["list-type=2", "versions", "uploads"] {
            let uri = format!("/bucket?{query}").parse().expect("list URI");
            assert!(is_bulk_request(
                &Method::GET,
                None,
                "/bucket",
                "s3.example",
                &uri,
                &headers,
            ));
        }
        for query in ["location", "replication", "versioning"] {
            let uri = format!("/bucket?{query}").parse().expect("config URI");
            assert!(!is_bulk_request(
                &Method::GET,
                None,
                "/bucket",
                "s3.example",
                &uri,
                &headers,
            ));
        }
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/%2F",
            "s3.example",
            &http::Uri::from_static("/bucket/%2F"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket//",
            "s3.example",
            &http::Uri::from_static("/bucket//"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/",
            "s3.example",
            &http::Uri::from_static("/bucket/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/key",
            "s3.example",
            &http::Uri::from_static("/key?partNumber=1&uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/",
            "s3.example",
            &http::Uri::from_static("/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/%2F",
            "s3.example",
            &http::Uri::from_static("/%2F"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploads"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploads&uploadId=upload"),
            &headers,
        ));
        let mut multipart = HeaderMap::new();
        multipart.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("Multipart/Form-Data; boundary=x"),
        );
        assert!(is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &multipart,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?tagging"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?tagging&uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
    }

    #[test]
    fn extracts_path_bucket() {
        assert_eq!(
            extract_bucket_name(
                Some("s3.example.com"),
                "/bucket-name/object.txt",
                "s3.example.com"
            ),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(Some("s3.example.com"), "/bucket-name", "s3.example.com"),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(None, "/bucket-name", "s3.example.com"),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(Some("s3.example.com"), "/", "s3.example.com"),
            None
        );
    }

    #[test]
    fn extracts_host_bucket() {
        assert_eq!(
            extract_bucket_name(
                Some("bucket-name.s3.example.com"),
                "/object.txt",
                "s3.example.com"
            ),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(
                Some("bucket-name.s3.example.com:9000"),
                "/object.txt",
                "s3.example.com:9000"
            ),
            Some("bucket-name".to_string())
        );
    }

    #[test]
    fn classifies_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://portal.test"),
        );
        headers.insert(
            header::ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("PUT"),
        );
        headers.insert(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            HeaderValue::from_static("Content-Type, X-Test"),
        );
        let preflight = RequestClassification::classify(
            &parts(Method::OPTIONS, "https://s3.example/bucket/key", headers),
            "s3.example",
        );
        assert!(preflight.origin_header.is_some());
        assert_eq!(preflight.requested_method.as_deref(), Some("PUT"));
        assert_eq!(
            preflight.requested_headers,
            vec!["content-type".to_string(), "x-test".to_string()]
        );
        assert!(!preflight.bulk_request);

        let mut oversized = HeaderMap::new();
        oversized.insert(header::CONTENT_LENGTH, HeaderValue::from_static("2097153"));
        let delete = RequestClassification::classify(
            &parts(Method::POST, "https://s3.example/bucket?delete", oversized),
            "s3.example",
        );
        assert!(delete.delete_objects);
        assert!(delete.oversized_delete);
        assert!(delete.bulk_request);

        let complete = RequestClassification::classify(
            &parts(
                Method::POST,
                "https://s3.example/bucket/key?uploadId=upload",
                HeaderMap::new(),
            ),
            "s3.example",
        );
        assert!(complete.complete_multipart);
        assert!(!complete.oversized_delete);
        assert!(complete.bulk_request);

        let invalid = RequestClassification::classify(
            &parts(Method::GET, "https://s3.example/AB/", HeaderMap::new()),
            "s3.example",
        );
        assert!(invalid.invalid_bucket.is_some());
    }
}
