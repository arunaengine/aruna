//! Tests injection, verification, response exposure and echo removal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::compute::Secret;

const CANARY: &str = "canary-8d2e";

fn fields(pairs: &[(&str, &str)]) -> SecretFields {
    SecretFields::new(
        (pairs.iter())
            .map(|(name, value)| (name.to_string(), Secret::new(*value)))
            .collect(),
    )
}

fn settings(pairs: &[(&str, &str)]) -> SecretSettings {
    (pairs.iter())
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn kind(name: &str) -> SecretKind {
    SecretKind::parse(name).unwrap()
}

fn request() -> OutgoingRequest {
    OutgoingRequest {
        method: "POST".to_string(),
        url: Url::parse("https://api.example.org/records").unwrap(),
        headers: vec![("Authorization".to_string(), "Bearer forged".to_string())],
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    (headers.iter())
        .find(|(held, _)| held.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[test]
fn injects_per_kind() {
    let mut bearer = request();
    inject_secret(
        &kind("bearer"),
        &settings(&[]),
        &fields(&[("token", CANARY)]),
        &mut bearer,
    )
    .unwrap();
    assert_eq!(
        bearer.headers,
        [("authorization".to_string(), format!("Bearer {CANARY}"))]
    );
    let mut basic = request();
    let pair = fields(&[("username", "user"), ("password", CANARY)]);
    inject_secret(&kind("basic"), &settings(&[]), &pair, &mut basic).unwrap();
    let encoded = STANDARD.encode(format!("user:{CANARY}"));
    assert_eq!(
        header(&basic.headers, "authorization"),
        Some(format!("Basic {encoded}").as_str())
    );
    let mut custom = request();
    let named = settings(&[("header", "X-Api-Key")]);
    inject_secret(
        &kind("header"),
        &named,
        &fields(&[("value", CANARY)]),
        &mut custom,
    )
    .unwrap();
    assert_eq!(header(&custom.headers, "x-api-key"), Some(CANARY));
    let refused = inject_secret(
        &kind("hmac"),
        &named,
        &fields(&[("key", CANARY)]),
        &mut custom,
    );
    assert_eq!(refused, Err(InjectError::NotInjectable));
}

#[test]
fn verify_strips_token() {
    let token = fields(&[("token", CANARY)]);
    let mut headers = vec![
        ("Authorization".to_string(), format!("Bearer {CANARY}")),
        ("Cookie".to_string(), "session=1".to_string()),
        ("Proxy-Authorization".to_string(), "Basic x".to_string()),
        ("Content-Type".to_string(), "application/json".to_string()),
    ];
    assert_eq!(
        verify_request(&kind("bearer"), &settings(&[]), &token, &mut headers, b"{}"),
        Ok(true)
    );
    assert_eq!(
        headers,
        [("Content-Type".to_string(), "application/json".to_string())]
    );
    let mut wrong = vec![(
        "Authorization".to_string(),
        "Bearer canary-0000".to_string(),
    )];
    assert_eq!(
        verify_request(&kind("bearer"), &settings(&[]), &token, &mut wrong, b""),
        Ok(false)
    );
    assert!(wrong.is_empty());
    let named = settings(&[("header", "X-Token")]);
    let mut custom = vec![("x-token".to_string(), CANARY.to_string())];
    let value = fields(&[("value", CANARY)]);
    assert_eq!(
        verify_request(&kind("header"), &named, &value, &mut custom, b""),
        Ok(true)
    );
    assert!(custom.is_empty());
}

#[test]
fn verify_checks_hmac() {
    let key = fields(&[("key", CANARY)]);
    let named = settings(&[("header", "X-Signature"), ("prefix", "sha256=")]);
    let body = br#"{"event":"created"}"#;
    let mut mac = Hmac::<Sha256>::new_from_slice(CANARY.as_bytes()).unwrap();
    mac.update(body);
    let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let mut valid = vec![("X-Signature".to_string(), signature.clone())];
    assert_eq!(
        verify_request(&kind("hmac"), &named, &key, &mut valid, body),
        Ok(true)
    );
    assert!(valid.is_empty());
    for (sent, body) in [
        (signature.as_str(), &b"{}"[..]),
        ("sha256=zz", body),
        ("", body),
    ] {
        let mut headers = vec![("X-Signature".to_string(), sent.to_string())];
        assert_eq!(
            verify_request(&kind("hmac"), &named, &key, &mut headers, body),
            Ok(false)
        );
    }
    let mut missing = Vec::new();
    assert_eq!(
        verify_request(&kind("hmac"), &named, &key, &mut missing, body),
        Ok(false)
    );
}

#[test]
fn exposure_limits_answer() {
    let token = fields(&[("token", CANARY)]);
    let headers = vec![
        ("Location".to_string(), "/records/42".to_string()),
        ("X-Debug".to_string(), CANARY.to_string()),
    ];
    let body = format!(r#"{{"id":42,"echo":"{CANARY}","links":{{"self":"/records/42"}}}}"#);
    let status = expose_response(
        &ResponseExposure::Status,
        &token,
        201,
        &headers,
        body.as_bytes(),
    );
    assert_eq!(
        (status.status, status.headers.len(), status.body),
        (201, 0, None)
    );
    let listed = ResponseExposure::Fields {
        pointers: vec![
            "/id".to_string(),
            "/echo".to_string(),
            "/missing".to_string(),
        ],
        headers: vec!["location".to_string()],
    };
    let picked = expose_response(&listed, &token, 201, &headers, body.as_bytes());
    assert_eq!(
        picked.headers,
        [("Location".to_string(), "/records/42".to_string())]
    );
    let json: serde_json::Value = serde_json::from_slice(&picked.body.unwrap()).unwrap();
    assert_eq!(
        json,
        serde_json::json!({ "/id": 42, "/echo": "[redacted]" })
    );
    let full = expose_response(
        &ResponseExposure::Full,
        &token,
        201,
        &headers,
        body.as_bytes(),
    );
    let shown = format!(
        "{:?}{}",
        full.headers,
        String::from_utf8(full.body.unwrap()).unwrap()
    );
    assert!(!shown.contains(CANARY) && shown.contains("[redacted]"));
}

#[test]
fn removes_encoded_echoes() {
    let tricky = "canary\"8d/2e+=";
    let pair = fields(&[("username", "user"), ("password", tricky)]);
    let basic = STANDARD.encode(format!("user:{tricky}"));
    let escaped = serde_json::to_string(tricky).unwrap();
    let body = [
        tricky.to_string(),
        escaped.trim_matches('"').to_string(),
        STANDARD.encode(tricky),
        URL_SAFE_NO_PAD.encode(tricky),
        utf8_percent_encode(tricky, NON_ALPHANUMERIC).to_string(),
        basic.clone(),
    ]
    .join(" ");
    let full = expose_response(&ResponseExposure::Full, &pair, 200, &[], body.as_bytes());
    let shown = String::from_utf8(full.body.unwrap()).unwrap();
    assert!(
        !shown.contains("canary") && !shown.contains(&basic),
        "{shown}"
    );
    // Short values are left alone, so ordinary text survives.
    let short = fields(&[("username", "ab"), ("password", "cd")]);
    let kept = expose_response(&ResponseExposure::Full, &short, 200, &[], b"abcd");
    assert_eq!(kept.body.unwrap(), b"abcd");
}

#[test]
fn redirects_status_only() {
    let token = fields(&[("token", CANARY)]);
    let headers = vec![(
        "Location".to_string(),
        "https://elsewhere.example.org".to_string(),
    )];
    let answer = expose_response(&ResponseExposure::Full, &token, 302, &headers, b"moved");
    assert_eq!(
        (answer.status, answer.headers.len(), answer.body),
        (302, 0, None)
    );
    let mut sent = request();
    sent.headers = vec![("authorization".to_string(), format!("Bearer {CANARY}"))];
    let shown = format!("{sent:?}");
    assert!(shown.contains("authorization") && !shown.contains(CANARY));
}
