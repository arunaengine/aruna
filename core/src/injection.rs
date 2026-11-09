//! Applies a released secret on the host: adds it to an approved request, checks incoming
//! requests with it, and limits what a consumer sees of an answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use url::Url;

use crate::secrets::{
    Injection, ResponseExposure, SecretFields, SecretKind, SecretSettings, Verification,
};

/// Headers that carry credentials; removed from an incoming request after its check.
const CREDENTIAL_HEADERS: &[&str] = &["authorization", "cookie", "proxy-authorization"];
/// Shorter values are not searched for in answers, since they would match ordinary text.
const MIN_ECHO: usize = 4;
const REDACTED: &[u8] = b"[redacted]";

/// An outgoing request the host sends for a consumer.
#[derive(Clone, PartialEq, Eq)]
pub struct OutgoingRequest {
    pub method: String,
    pub url: Url,
    pub headers: Vec<(String, String)>,
}

impl std::fmt::Debug for OutgoingRequest {
    /// Header values may hold an injected secret, so only their names are shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("OutgoingRequest")
            .field("method", &self.method)
            .field("url", &self.url.as_str())
            .field("headers", &names)
            .finish()
    }
}

/// Why a released secret could not be applied. Never carries a value.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum InjectError {
    #[error("this secret kind cannot be added to requests")]
    NotInjectable,
    #[error("this secret kind cannot check incoming requests")]
    NotVerifiable,
    #[error("the secret lacks a field or setting its kind needs")]
    Incomplete,
}

/// Adds the secret to `request` as its kind says, replacing a header of the same name.
/// The caller has matched the use rules for this request (`secrets::check_use`).
pub fn inject_secret(
    kind: &SecretKind,
    settings: &SecretSettings,
    fields: &SecretFields,
    request: &mut OutgoingRequest,
) -> Result<(), InjectError> {
    let spec = kind.spec().map_err(|_| InjectError::NotInjectable)?;
    let field = |name| fields.get(name).ok_or(InjectError::Incomplete);
    let (name, value) = match spec.inject.ok_or(InjectError::NotInjectable)? {
        Injection::Bearer => (
            "authorization".to_string(),
            format!("Bearer {}", field("token")?),
        ),
        Injection::Basic => {
            let pair = format!("{}:{}", field("username")?, field("password")?);
            (
                "authorization".to_string(),
                format!("Basic {}", STANDARD.encode(pair)),
            )
        }
        Injection::Header => {
            let header = settings.get("header").ok_or(InjectError::Incomplete)?;
            (header.to_ascii_lowercase(), field("value")?.to_string())
        }
    };
    (request.headers).retain(|(held, _)| !held.eq_ignore_ascii_case(&name));
    request.headers.push((name, value));
    Ok(())
}

/// Checks an incoming request in constant time, then removes the checked header and every
/// credential header, so the consumer never sees a token, whether the check passed or not.
pub fn verify_request(
    kind: &SecretKind,
    settings: &SecretSettings,
    fields: &SecretFields,
    headers: &mut Vec<(String, String)>,
    body: &[u8],
) -> Result<bool, InjectError> {
    let spec = kind.spec().map_err(|_| InjectError::NotVerifiable)?;
    let verification = spec.verify.ok_or(InjectError::NotVerifiable)?;
    let header = match verification {
        Verification::Bearer => "authorization".to_string(),
        Verification::Header | Verification::Hmac => {
            (settings.get("header").ok_or(InjectError::Incomplete)?).to_ascii_lowercase()
        }
    };
    let received = (headers.iter())
        .find(|(name, _)| name.eq_ignore_ascii_case(&header))
        .map(|(_, value)| value.clone());
    let field = |name| fields.get(name).ok_or(InjectError::Incomplete);
    let passed = match (verification, received) {
        (_, None) => false,
        (Verification::Bearer, Some(value)) => same(
            value.as_bytes(),
            format!("Bearer {}", field("token")?).as_bytes(),
        ),
        (Verification::Header, Some(value)) => same(value.as_bytes(), field("value")?.as_bytes()),
        (Verification::Hmac, Some(value)) => {
            let prefix = settings.get("prefix").map_or("", String::as_str);
            let digest = value
                .strip_prefix(prefix)
                .and_then(|hex| hex::decode(hex).ok());
            let mut mac = Hmac::<Sha256>::new_from_slice(field("key")?.as_bytes())
                .map_err(|_| InjectError::Incomplete)?;
            mac.update(body);
            digest.is_some_and(|digest| mac.verify_slice(&digest).is_ok())
        }
    };
    headers.retain(|(name, _)| {
        let name = name.to_ascii_lowercase();
        name != header && !CREDENTIAL_HEADERS.contains(&name.as_str())
    });
    Ok(passed)
}

fn same(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

/// What a consumer sees of the answer to an injected request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExposedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// Limits an answer to the rule's exposure, then removes echoes of the secret as extra
/// protection. The removal cannot guarantee secrecy; the exposure is the contract.
pub fn expose_response(
    exposure: &ResponseExposure,
    fields: &SecretFields,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> ExposedResponse {
    // Redirects are not followed with a secret, so their headers and body never reach the consumer.
    let redirect = (300..400).contains(&status);
    let shown = match redirect {
        true => &ResponseExposure::Status,
        false => exposure,
    };
    let (headers, body) = match shown {
        ResponseExposure::Status => (Vec::new(), None),
        ResponseExposure::Full => (headers.to_vec(), Some(body.to_vec())),
        ResponseExposure::Fields {
            pointers,
            headers: listed,
        } => {
            let kept = (headers.iter())
                .filter(|(name, _)| listed.iter().any(|held| held.eq_ignore_ascii_case(name)))
                .cloned()
                .collect();
            let json = serde_json::from_slice::<serde_json::Value>(body).ok();
            let picked: serde_json::Map<_, _> = (pointers.iter())
                .filter_map(|pointer| {
                    let value = json.as_ref()?.pointer(pointer)?;
                    Some((pointer.clone(), value.clone()))
                })
                .collect();
            (kept, serde_json::to_vec(&picked).ok())
        }
    };
    let echoes = echo_forms(fields);
    ExposedResponse {
        status,
        headers: (headers.into_iter())
            .map(|(name, value)| {
                let value = redact(value.as_bytes(), &echoes);
                (name, String::from_utf8_lossy(&value).into_owned())
            })
            .collect(),
        body: body.map(|body| redact(&body, &echoes)),
    }
}

/// Exact, JSON escaped, base64 and percent-encoded forms of each field and of the Basic pair.
fn echo_forms(fields: &SecretFields) -> Vec<Vec<u8>> {
    let mut plain: Vec<String> = fields.values().map(str::to_string).collect();
    if let (Some(user), Some(password)) = (fields.get("username"), fields.get("password")) {
        plain.push(format!("{user}:{password}"));
    }
    let mut forms = Vec::new();
    for value in plain.iter().filter(|value| value.len() >= MIN_ECHO) {
        let escaped = serde_json::to_string(value).unwrap_or_default();
        forms.push(value.as_bytes().to_vec());
        forms.push(escaped.trim_matches('"').as_bytes().to_vec());
        forms.push(STANDARD.encode(value).into_bytes());
        forms.push(URL_SAFE.encode(value).into_bytes());
        forms.push(URL_SAFE_NO_PAD.encode(value).into_bytes());
        forms.push(
            utf8_percent_encode(value, NON_ALPHANUMERIC)
                .to_string()
                .into_bytes(),
        );
    }
    // Longer forms first, so a form inside another is not left half replaced.
    forms.sort_by_key(|form| std::cmp::Reverse(form.len()));
    forms.dedup();
    forms
}

fn redact(text: &[u8], forms: &[Vec<u8>]) -> Vec<u8> {
    let mut text = text.to_vec();
    for form in forms {
        let mut out = Vec::with_capacity(text.len());
        let mut rest = text.as_slice();
        while let Some(at) = rest.windows(form.len()).position(|window| window == form) {
            out.extend_from_slice(&rest[..at]);
            out.extend_from_slice(REDACTED);
            rest = &rest[at + form.len()..];
        }
        out.extend_from_slice(rest);
        text = out;
    }
    text
}

#[cfg(test)]
#[path = "injection_tests.rs"]
mod tests;
