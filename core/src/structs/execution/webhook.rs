//! Defines a user's webhook setting, the delivery job of one notification and its signature.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::register::RegisterStamp;
use crate::structs::execution::notification::{NotificationKind, NotificationRecord};

/// The one event a webhook delivers: an upload through one of the subscriber's share links.
pub const WEBHOOK_EVENT: &str = "share.upload";
/// Header carrying `t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`.
pub const SIGNATURE_HEADER: &str = "x-aruna-signature";
/// Header carrying the delivery id, stable across retries, for receiver deduplication.
pub const DELIVERY_HEADER: &str = "x-aruna-delivery";

/// The most bytes a webhook URL may take.
pub const URL_BYTES: usize = 2048;

/// One immutable write of a user's webhook setting register, replicated in the realm topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscription {
    pub subscription_id: Ulid,
    pub user_id: UserId,
    /// The register order; its write id is `subscription_id`.
    pub stamp: RegisterStamp,
    /// `None` when the webhook is off.
    pub url: Option<String>,
    /// The signing secret sealed to the realm key; `None` turns the webhook off.
    pub sealed: Option<Vec<u8>>,
    pub created_at_ms: u64,
}

impl WebhookSubscription {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// An http or https URL with a host and without credentials, at most [`URL_BYTES`] long.
pub fn valid_url(url: &str) -> bool {
    url.len() <= URL_BYTES
        && url::Url::parse(url).is_ok_and(|parsed| {
            matches!(parsed.scheme(), "http" | "https")
                && parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.host().is_some()
        })
}

/// One delivery of one notification; the job reads the current setting when it runs, so
/// `subscription_id` is nil.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliverySpec {
    pub subscription_id: Ulid,
    pub notification: NotificationRecord,
}

impl WebhookDeliverySpec {
    /// The body sent for the selected event; `None` for every other notification.
    pub fn body(&self) -> Option<Value> {
        let record = &self.notification;
        let NotificationKind::ShareUpload {
            share_id,
            node_id,
            group_id,
            bucket,
            key,
            size_bytes,
        } = &record.kind
        else {
            return None;
        };
        Some(json!({
            "event": WEBHOOK_EVENT,
            "delivery_id": record.notification_id.to_string(),
            "occurred_at_ms": record.created_at_ms,
            "share_id": share_id.to_string(),
            "node_id": node_id.to_string(),
            "group_id": group_id.to_string(),
            "bucket": bucket,
            "key": key,
            "size_bytes": size_bytes,
        }))
    }
}

/// The value of [`SIGNATURE_HEADER`] for `body` sent at `timestamp` seconds.
pub fn sign_delivery(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts keys of every length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!(
        "t={timestamp},v1={}",
        hex::encode(mac.finalize().into_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::execution::notification::NotificationClass;
    use crate::structs::identity::realm::RealmId;

    fn record(kind: NotificationKind) -> NotificationRecord {
        NotificationRecord {
            notification_id: Ulid::from_parts(7, 7),
            recipient: UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([1; 32])),
            class: NotificationClass::Direct,
            kind,
            created_at_ms: 42,
            read_at_ms: None,
            watch_authorization: None,
        }
    }

    #[test]
    fn body_names_upload() {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let spec = WebhookDeliverySpec {
            subscription_id: Ulid::from_parts(2, 2),
            notification: record(NotificationKind::ShareUpload {
                share_id: Ulid::from_parts(4, 4),
                node_id,
                group_id: Ulid::from_parts(5, 5),
                bucket: "inbox".to_string(),
                key: "a.csv".to_string(),
                size_bytes: 9,
            }),
        };
        let body = spec.body().unwrap();
        assert_eq!(body["event"], WEBHOOK_EVENT);
        assert_eq!(body["delivery_id"], Ulid::from_parts(7, 7).to_string());
        assert_eq!(body["key"], "a.csv");
        let other = WebhookDeliverySpec {
            notification: record(NotificationKind::BucketDeleted {
                bucket: "inbox".to_string(),
                node_id,
                group_id: Ulid::from_parts(5, 5),
            }),
            ..spec
        };
        assert_eq!(other.body(), None);
    }

    #[test]
    fn signature_matches_hmac() {
        // An independent HMAC over "<t>.<body>" agrees, and another timestamp differs.
        let signed = sign_delivery(b"Jefe", 1, b"what do ya want for nothing?");
        let mut mac = Hmac::<Sha256>::new_from_slice(b"Jefe").unwrap();
        mac.update(b"1.what do ya want for nothing?");
        let expected = hex::encode(mac.finalize().into_bytes());
        assert_eq!(signed, format!("t=1,v1={expected}"));
        assert_ne!(
            signed,
            sign_delivery(b"Jefe", 2, b"what do ya want for nothing?")
        );
    }
}
