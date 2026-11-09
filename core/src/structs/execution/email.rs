//! Defines a user's email notification setting, the delivery job of one notice and its text.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::realm_vault::{RealmSecretRef, RealmVaultPurpose};
use crate::structs::execution::notification::NotificationRecord;

/// The notification categories a user may receive by email.
pub const EMAIL_CATEGORIES: &[&str] = &["group.membership", "share.upload", "bucket.encryption"];

/// One immutable version of a user's email setting, replicated in the realm topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailPreference {
    pub preference_id: Ulid,
    pub user_id: UserId,
    /// A write takes one above every version a quorum of management nodes holds.
    pub version: u64,
    /// Empty when email is off.
    pub categories: BTreeSet<String>,
    /// The address sealed to the realm key; `None` turns email off.
    pub sealed: Option<Vec<u8>>,
    pub created_at_ms: u64,
}

impl EmailPreference {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn wants(&self, record: &NotificationRecord) -> bool {
        self.categories.contains(record.kind.category())
    }
}

/// How a connection to the mail server is secured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SmtpTls {
    /// Plain connection upgraded with STARTTLS, which must succeed.
    Starttls,
    /// TLS from the first byte.
    Tls,
    /// No TLS, for a relay on the same host or network only.
    Plain,
}

/// The SMTP profile every infrastructure node of the realm sends notification emails with.
/// The password is a realm vault entry; the profile only points at one revision of it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmEmailProfile {
    /// New on every change; nodes key their mail sender by it.
    pub revision: Ulid,
    pub host: String,
    pub port: Option<u16>,
    pub tls: SmtpTls,
    pub username: Option<String>,
    pub from: String,
    /// `None` sends without SMTP authentication.
    pub password: Option<RealmSecretRef>,
    pub changed_by: UserId,
    pub changed_at_ms: u64,
}

impl RealmEmailProfile {
    /// A host and sender are set, a password comes with a username and is an SMTP password entry.
    pub fn valid(&self) -> bool {
        let filled = |text: &str| !text.trim().is_empty() && !text.chars().any(char::is_control);
        filled(&self.host)
            && filled(&self.from)
            && self.username.as_deref().is_none_or(filled)
            && (self.password).is_none_or(|password| {
                self.username.is_some() && password.entry.purpose == RealmVaultPurpose::SmtpPassword
            })
    }
}

/// One email for one notification; the job reads the current setting when it runs, so
/// `preference_id` is nil.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailDeliverySpec {
    pub preference_id: Ulid,
    pub notification: NotificationRecord,
}

impl EmailDeliverySpec {
    /// Subject and plain text body. The inbox stays authoritative, so the mail only points to
    /// it and carries no names, paths or object keys.
    pub fn text(&self) -> (String, String) {
        let subject = match self.notification.kind.category() {
            "group.membership" => "Aruna: a group membership changed",
            "share.upload" => "Aruna: a file arrived through your share link",
            "bucket.encryption" => "Aruna: an encrypted bucket needs attention",
            _ => "Aruna: a new notification",
        };
        let body = format!(
            "You have a new Aruna notification ({}).\n\nOpen your Aruna notifications to see it.\n\n\
             You get this email because you turned on email notifications. Turn them off in your \
             notification settings.\n",
            self.notification.kind.name()
        );
        (subject.to_string(), body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::execution::notification::{NotificationClass, NotificationKind};
    use crate::structs::identity::realm::RealmId;

    #[test]
    fn text_hides_details() {
        let record = NotificationRecord {
            notification_id: Ulid::from_parts(7, 7),
            recipient: UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([1; 32])),
            class: NotificationClass::Direct,
            kind: NotificationKind::ShareUpload {
                share_id: Ulid::from_parts(4, 4),
                node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
                group_id: Ulid::from_parts(5, 5),
                bucket: "inbox".to_string(),
                key: "patients/secret.csv".to_string(),
                size_bytes: 9,
            },
            created_at_ms: 42,
            read_at_ms: None,
            watch_authorization: None,
        };
        let spec = EmailDeliverySpec {
            preference_id: Ulid::from_parts(2, 2),
            notification: record.clone(),
        };
        let (subject, body) = spec.text();
        assert!(subject.contains("share link"));
        assert!(!body.contains("secret.csv") && !body.contains("inbox"));
        let preference = EmailPreference {
            preference_id: Ulid::from_parts(2, 2),
            user_id: record.recipient,
            version: 1,
            categories: BTreeSet::from(["group.membership".to_string()]),
            sealed: Some(vec![1]),
            created_at_ms: 1,
        };
        assert!(!preference.wants(&record));
    }
}
