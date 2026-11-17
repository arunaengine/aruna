//! Builds the CEL policy request of one authorized action from its path, permission, caller and
//! extra context.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::request_policy::{PolicyRequest, PolicySession};
use aruna_core::structs::identity::auth::AuthContext;

/// Extra request context threaded to a policy at a choke point. Body-content
/// policies only ever run for operations whose handler already holds the full
/// parsed body; the engine never reads or buffers a streaming body.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PolicyRequestExtras {
    pub operation: String,
    pub params: std::collections::BTreeMap<String, String>,
    pub headers: std::collections::BTreeMap<String, String>,
    // Encoded as an optional JSON string so non-self-describing wire formats
    // such as postcard can carry the body across the mirror messages.
    #[serde(with = "json_body")]
    pub body: Option<serde_json::Value>,
}

mod json_body {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<serde_json::Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .as_ref()
            .map(serde_json::Value::to_string)
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|text| serde_json::from_str(&text).map_err(serde::de::Error::custom))
            .transpose()
    }
}

impl PolicyRequestExtras {
    /// Generic REST context with no parameters, headers, or body.
    pub fn rest() -> Self {
        Self {
            operation: "rest".to_string(),
            ..Default::default()
        }
    }

    /// Context carrying only an operation identifier.
    pub fn operation(operation: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            ..Default::default()
        }
    }
}

/// Builds the policy request for one authorized action.
pub fn policy_request(
    path: &str,
    permission: &aruna_core::structs::identity::auth::Permission,
    user: Option<&aruna_core::UserId>,
) -> PolicyRequest {
    PolicyRequest::basic(
        path.to_string(),
        permission_label(permission),
        user.filter(|user| !user.is_nil())
            .map(|user| user.to_string())
            .unwrap_or_default(),
    )
}

/// Builds a policy request that additionally carries operation, parameters,
/// headers, and an already-parsed body.
pub fn policy_request_with(
    path: &str,
    permission: &aruna_core::structs::identity::auth::Permission,
    auth: Option<&AuthContext>,
    extras: PolicyRequestExtras,
) -> PolicyRequest {
    let mut request = policy_request(path, permission, auth.map(|auth| &auth.user_id));
    if !extras.operation.is_empty() {
        request.operation = extras.operation;
    }
    request.params = extras.params;
    request.headers = extras.headers;
    request.body = extras.body;
    request.session = auth.and_then(|auth| {
        auth.session.as_ref().map(|session| PolicySession {
            sid: session.sid.clone(),
            kind: session.kind.to_string(),
            label: String::new(),
        })
    });
    request
}

fn permission_label(permission: &aruna_core::structs::identity::auth::Permission) -> String {
    match permission {
        aruna_core::structs::identity::auth::Permission::READ => "read".to_string(),
        aruna_core::structs::identity::auth::Permission::WRITE => "write".to_string(),
        aruna_core::structs::identity::auth::Permission::DENY => "deny".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extras_set_operation() {
        // Extras carry the operation name and body into the policy request.
        let extras = PolicyRequestExtras::operation("s3.PutObject");
        let request = policy_request_with(
            "/r/g/x/data/o",
            &aruna_core::structs::identity::auth::Permission::WRITE,
            None,
            extras,
        );
        assert_eq!(request.operation, "s3.PutObject");
        assert_eq!(request.permission, "write");
    }

    #[test]
    fn extras_survive_postcard() {
        // The mirror messages carry extras over postcard, which cannot decode a
        // bare serde_json::Value; the body must ride as an encoded string.
        let mut params = std::collections::BTreeMap::new();
        params.insert("versionId".to_string(), "1".to_string());
        let extras = PolicyRequestExtras {
            operation: "s3.PutBucketReplication".to_string(),
            params,
            headers: std::collections::BTreeMap::new(),
            body: Some(serde_json::json!({"rule": "deny"})),
        };
        let bytes = postcard::to_allocvec(&extras).unwrap();
        let decoded: PolicyRequestExtras = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.operation, extras.operation);
        assert_eq!(decoded.params, extras.params);
        assert_eq!(decoded.body, extras.body);
    }
}
