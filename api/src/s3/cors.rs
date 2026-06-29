use aruna_core::structs::{BucketCorsConfiguration, BucketCorsRule};
use http::header::{HeaderName, HeaderValue, VARY};
use http::{Method, StatusCode};
use s3s::HttpResponse;
use s3s::dto::{CORSConfiguration, CORSRule, GetBucketCorsOutput};
use s3s::{S3Error, s3_error};

pub(crate) const ORIGIN_HEADER: &str = "Origin";
pub(crate) const REQUEST_METHOD_HEADER: &str = "Access-Control-Request-Method";
pub(crate) const REQUEST_HEADERS_HEADER: &str = "Access-Control-Request-Headers";

pub(crate) const ALLOW_ORIGIN_HEADER: &str = "access-control-allow-origin";
pub(crate) const ALLOW_METHODS_HEADER: &str = "access-control-allow-methods";
pub(crate) const ALLOW_HEADERS_HEADER: &str = "access-control-allow-headers";
pub(crate) const EXPOSE_HEADERS_HEADER: &str = "access-control-expose-headers";
pub(crate) const MAX_AGE_HEADER: &str = "access-control-max-age";

const WILDCARD: &str = "*";
const VALID_CORS_METHODS: &[&str] = &["GET", "PUT", "HEAD", "POST", "DELETE"];

pub(crate) const PREFLIGHT_VARY: &[&str] =
    &[ORIGIN_HEADER, REQUEST_METHOD_HEADER, REQUEST_HEADERS_HEADER];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchedCorsRule {
    pub allow_origin: String,
    pub allow_methods: Vec<String>,
    pub allow_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<i32>,
}

pub(crate) fn dto_to_bucket_cors(
    input: CORSConfiguration,
) -> Result<BucketCorsConfiguration, S3Error> {
    if input.cors_rules.is_empty() {
        return Err(s3_error!(
            MalformedXML,
            "CORS configuration must contain at least one rule"
        ));
    }

    let rules = input
        .cors_rules
        .into_iter()
        .map(dto_rule_to_bucket_rule)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(BucketCorsConfiguration { rules })
}

pub(crate) fn bucket_cors_to_get_output(config: BucketCorsConfiguration) -> GetBucketCorsOutput {
    let cors_rules = config
        .rules
        .into_iter()
        .map(bucket_rule_to_dto_rule)
        .collect();

    GetBucketCorsOutput {
        cors_rules: Some(cors_rules),
    }
}

pub(crate) fn match_preflight_rule(
    config: &BucketCorsConfiguration,
    origin: &str,
    requested_method: &str,
    requested_headers: &[String],
) -> Option<MatchedCorsRule> {
    config.rules.iter().find_map(|rule| {
        if !origin_matches(rule, origin)
            || !method_matches(rule, requested_method)
            || !headers_match(rule, requested_headers)
        {
            return None;
        }

        Some(MatchedCorsRule {
            allow_origin: matched_origin(rule, origin),
            allow_methods: rule.allowed_methods.clone(),
            allow_headers: matched_allowed_headers(rule, requested_headers),
            expose_headers: rule.expose_headers.clone(),
            max_age_seconds: rule.max_age_seconds,
        })
    })
}

pub(crate) fn match_actual_rule(
    config: &BucketCorsConfiguration,
    origin: &str,
    method: &Method,
) -> Option<MatchedCorsRule> {
    let method = method.as_str();

    config.rules.iter().find_map(|rule| {
        if !origin_matches(rule, origin) || !method_matches(rule, method) {
            return None;
        }

        Some(MatchedCorsRule {
            allow_origin: matched_origin(rule, origin),
            allow_methods: rule.allowed_methods.clone(),
            allow_headers: rule.allowed_headers.clone(),
            expose_headers: rule.expose_headers.clone(),
            max_age_seconds: rule.max_age_seconds,
        })
    })
}

pub(crate) fn parse_requested_headers(raw_headers: &str) -> Vec<String> {
    raw_headers
        .split(',')
        .map(str::trim)
        .filter(|header| !header.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn dto_rule_to_bucket_rule(rule: CORSRule) -> Result<BucketCorsRule, S3Error> {
    if rule.allowed_methods.is_empty() || rule.allowed_origins.is_empty() {
        return Err(s3_error!(
            MalformedXML,
            "Each CORS rule must contain allowed methods and origins"
        ));
    }
    if rule
        .max_age_seconds
        .is_some_and(|max_age_seconds| max_age_seconds < 0)
    {
        return Err(s3_error!(
            MalformedXML,
            "CORS max age seconds must not be negative"
        ));
    }

    let allowed_methods = rule
        .allowed_methods
        .into_iter()
        .map(normalize_method)
        .collect::<Result<Vec<_>, _>>()?;
    let allowed_origins = rule
        .allowed_origins
        .into_iter()
        .map(|origin| normalize_non_empty(origin, "CORS allowed origin"))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(BucketCorsRule {
        id: rule.id,
        allowed_origins,
        allowed_methods,
        allowed_headers: normalize_optional_values(rule.allowed_headers, "CORS allowed header")?,
        expose_headers: normalize_optional_values(rule.expose_headers, "CORS expose header")?,
        max_age_seconds: rule.max_age_seconds,
    })
}

fn bucket_rule_to_dto_rule(rule: BucketCorsRule) -> CORSRule {
    CORSRule {
        allowed_headers: (!rule.allowed_headers.is_empty())
            .then_some(rule.allowed_headers.into_iter().collect()),
        allowed_methods: rule.allowed_methods.into_iter().collect(),
        allowed_origins: rule.allowed_origins.into_iter().collect(),
        expose_headers: (!rule.expose_headers.is_empty())
            .then_some(rule.expose_headers.into_iter().collect()),
        id: rule.id,
        max_age_seconds: rule.max_age_seconds,
    }
}

fn normalize_method(method: String) -> Result<String, S3Error> {
    let method = normalize_non_empty(method, "CORS allowed method")?.to_ascii_uppercase();
    if VALID_CORS_METHODS.contains(&method.as_str()) {
        Ok(method)
    } else {
        Err(s3_error!(MalformedXML, "Invalid CORS method `{method}`"))
    }
}

fn normalize_optional_values(
    values: Option<Vec<String>>,
    field_name: &'static str,
) -> Result<Vec<String>, S3Error> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|value| normalize_non_empty(value, field_name))
        .collect()
}

fn normalize_non_empty(value: String, field_name: &'static str) -> Result<String, S3Error> {
    let value = value.trim().to_string();
    if value.is_empty() {
        Err(s3_error!(MalformedXML, "{field_name} must not be empty"))
    } else {
        Ok(value)
    }
}

fn origin_matches(rule: &BucketCorsRule, origin: &str) -> bool {
    rule.allowed_origins
        .iter()
        .any(|allowed| pattern_matches(allowed, origin, false))
}

fn method_matches(rule: &BucketCorsRule, method: &str) -> bool {
    rule.allowed_methods
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(method))
}

fn headers_match(rule: &BucketCorsRule, requested_headers: &[String]) -> bool {
    requested_headers
        .iter()
        .all(|requested| header_allowed(rule, requested))
}

fn header_allowed(rule: &BucketCorsRule, requested_header: &str) -> bool {
    if rule.allowed_headers.is_empty() {
        return false;
    }

    rule.allowed_headers
        .iter()
        .any(|allowed| pattern_matches(allowed, requested_header, true))
}

fn matched_allowed_headers(rule: &BucketCorsRule, requested_headers: &[String]) -> Vec<String> {
    if requested_headers.is_empty() {
        return rule.allowed_headers.clone();
    }

    requested_headers.to_vec()
}

fn matched_origin(rule: &BucketCorsRule, origin: &str) -> String {
    if rule
        .allowed_origins
        .iter()
        .any(|allowed| allowed == WILDCARD)
    {
        WILDCARD.to_string()
    } else {
        origin.to_string()
    }
}

fn pattern_matches(pattern: &str, value: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        return wildcard_match(&pattern.to_ascii_lowercase(), &value.to_ascii_lowercase());
    }
    wildcard_match(pattern, value)
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == WILDCARD {
        return true;
    }

    match pattern.split_once(WILDCARD) {
        Some((prefix, suffix)) => {
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len()
        }
        None => pattern == value,
    }
}

pub(crate) fn build_preflight_response(matched_rule: MatchedCorsRule) -> HttpResponse {
    let mut response = HttpResponse::new(s3s::Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    append_allow_origin_and_methods(&mut response, &matched_rule);
    if !matched_rule.allow_headers.is_empty() {
        append_header(
            &mut response,
            HeaderName::from_static(ALLOW_HEADERS_HEADER),
            &matched_rule.allow_headers.join(", "),
        );
    }
    if let Some(max_age_seconds) = matched_rule.max_age_seconds {
        append_header(
            &mut response,
            HeaderName::from_static(MAX_AGE_HEADER),
            &max_age_seconds.to_string(),
        );
    }
    append_vary_headers(response.headers_mut(), PREFLIGHT_VARY);
    response
}

pub(crate) fn build_preflight_forbidden_response() -> HttpResponse {
    let mut response = HttpResponse::new(s3s::Body::empty());
    *response.status_mut() = StatusCode::FORBIDDEN;
    append_vary_headers(response.headers_mut(), PREFLIGHT_VARY);
    response
}

pub(crate) fn inject_actual_cors_headers(
    response: &mut HttpResponse,
    matched_rule: MatchedCorsRule,
) {
    append_allow_origin_and_methods(response, &matched_rule);
    if !matched_rule.expose_headers.is_empty() {
        append_header(
            response,
            HeaderName::from_static(EXPOSE_HEADERS_HEADER),
            &matched_rule.expose_headers.join(", "),
        );
    }
    append_vary_headers(response.headers_mut(), &[ORIGIN_HEADER]);
}

fn append_allow_origin_and_methods(response: &mut HttpResponse, matched_rule: &MatchedCorsRule) {
    append_header(
        response,
        HeaderName::from_static(ALLOW_ORIGIN_HEADER),
        &matched_rule.allow_origin,
    );
    append_header(
        response,
        HeaderName::from_static(ALLOW_METHODS_HEADER),
        &matched_rule.allow_methods.join(", "),
    );
}

fn append_header<T>(response: &mut http::Response<T>, name: HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().insert(name, value);
    }
}

fn append_vary_headers(headers: &mut http::HeaderMap, values: &[&str]) {
    let mut vary_values = headers
        .get(VARY)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    for value in values {
        if !vary_values
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(value))
        {
            vary_values.push((*value).to_string());
        }
    }

    if !vary_values.is_empty()
        && let Ok(value) = HeaderValue::from_str(&vary_values.join(", "))
    {
        headers.insert(VARY, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_cors_configuration_between_dto_and_core() {
        let dto = CORSConfiguration {
            cors_rules: vec![CORSRule {
                allowed_headers: Some(vec!["content-type".to_string(), "x-amz-meta-*".to_string()]),
                allowed_methods: vec!["GET".to_string(), "PUT".to_string()],
                allowed_origins: vec!["https://example.org".to_string()],
                expose_headers: Some(vec!["etag".to_string()]),
                id: Some("rule-1".to_string()),
                max_age_seconds: Some(60),
            }],
        };

        let core = dto_to_bucket_cors(dto.clone()).unwrap();
        assert_eq!(core.rules.len(), 1);
        assert_eq!(core.rules[0].allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(core.rules[0].allowed_origins, vec!["https://example.org"]);

        let output = bucket_cors_to_get_output(core);
        let roundtrip = CORSConfiguration {
            cors_rules: output.cors_rules.unwrap(),
        };
        assert_eq!(roundtrip, dto);
    }

    #[test]
    fn rejects_invalid_cors_configuration() {
        let err = dto_to_bucket_cors(CORSConfiguration { cors_rules: vec![] }).unwrap_err();
        assert_eq!(err.code(), &s3s::S3ErrorCode::MalformedXML);

        let err = dto_to_bucket_cors(CORSConfiguration {
            cors_rules: vec![CORSRule {
                allowed_headers: None,
                allowed_methods: vec!["OPTIONS".to_string()],
                allowed_origins: vec!["https://example.org".to_string()],
                expose_headers: None,
                id: None,
                max_age_seconds: None,
            }],
        })
        .unwrap_err();
        assert_eq!(err.code(), &s3s::S3ErrorCode::MalformedXML);
    }

    #[test]
    fn matches_preflight_rules_with_case_insensitive_header_checks() {
        let config = BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: None,
                allowed_origins: vec!["https://*.example.org".to_string()],
                allowed_methods: vec!["GET".to_string(), "PUT".to_string()],
                allowed_headers: vec!["content-type".to_string(), "x-amz-meta-*".to_string()],
                expose_headers: vec![],
                max_age_seconds: Some(300),
            }],
        };

        let matched = match_preflight_rule(
            &config,
            "https://bucket.example.org",
            "PUT",
            &parse_requested_headers("Content-Type, X-Amz-Meta-Test"),
        )
        .unwrap();

        assert_eq!(matched.allow_origin, "https://bucket.example.org");
        assert_eq!(
            matched.allow_headers,
            vec!["content-type", "x-amz-meta-test"]
        );
        assert_eq!(matched.max_age_seconds, Some(300));
    }

    #[test]
    fn matches_actual_rules_with_wildcard_origin() {
        let config = BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: None,
                allowed_origins: vec!["*".to_string()],
                allowed_methods: vec!["GET".to_string()],
                allowed_headers: vec![],
                expose_headers: vec!["etag".to_string()],
                max_age_seconds: None,
            }],
        };

        let matched = match_actual_rule(&config, "https://example.org", &Method::GET).unwrap();

        assert_eq!(matched.allow_origin, "*");
        assert_eq!(matched.expose_headers, vec!["etag"]);
    }

    #[test]
    fn rejects_preflight_when_requested_header_is_not_allowed() {
        let config = BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: None,
                allowed_origins: vec!["https://example.org".to_string()],
                allowed_methods: vec!["PUT".to_string()],
                allowed_headers: vec!["content-type".to_string()],
                expose_headers: vec![],
                max_age_seconds: None,
            }],
        };

        let matched = match_preflight_rule(
            &config,
            "https://example.org",
            "PUT",
            &parse_requested_headers("x-custom-header"),
        );

        assert!(matched.is_none());
    }

    fn matched_rule() -> MatchedCorsRule {
        MatchedCorsRule {
            allow_origin: "https://example.org".to_string(),
            allow_methods: vec!["GET".to_string(), "PUT".to_string()],
            allow_headers: vec!["content-type".to_string(), "x-test".to_string()],
            expose_headers: vec!["etag".to_string()],
            max_age_seconds: Some(60),
        }
    }

    #[test]
    fn builds_preflight_success_response_with_expected_headers() {
        let response = build_preflight_response(matched_rule());

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "https://example.org"
        );
        assert_eq!(
            response.headers()["access-control-allow-methods"],
            "GET, PUT"
        );
        assert_eq!(
            response.headers()["access-control-allow-headers"],
            "content-type, x-test"
        );
        assert_eq!(response.headers()["access-control-max-age"], "60");
        assert_eq!(
            response.headers()[VARY],
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
        );
    }

    #[test]
    fn builds_preflight_failure_response_without_allow_headers() {
        let response = build_preflight_forbidden_response();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
        assert_eq!(
            response.headers()[VARY],
            "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
        );
    }

    #[test]
    fn injects_actual_cors_headers_and_preserves_existing_vary_entries() {
        let mut response = HttpResponse::new(s3s::Body::empty());
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("Accept-Encoding"));

        inject_actual_cors_headers(&mut response, matched_rule());

        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "https://example.org"
        );
        assert_eq!(response.headers()["access-control-expose-headers"], "etag");
        assert_eq!(response.headers()[VARY], "Accept-Encoding, Origin");
    }
}
