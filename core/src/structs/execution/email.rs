//! Defines a user's email notification opt-in, the delivery job of one notice and its text.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::structs::execution::notification::NotificationRecord;

/// The notification categories a user may receive by email.
pub const EMAIL_CATEGORIES: &[&str] = &["group.membership", "share.upload", "bucket.encryption"];

/// One opt-in. The address itself is sealed in the node vault under `preference_id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmailPreference {
    pub preference_id: Ulid,
    pub user_id: UserId,
    pub categories: BTreeSet<String>,
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

pub fn email_key(user_id: &UserId) -> Vec<u8> {
    user_id.to_bytes()
}

/// One email for one notification to the address of `preference_id`.
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
            categories: BTreeSet::from(["group.membership".to_string()]),
            created_at_ms: 1,
        };
        assert!(!preference.wants(&record));
    }
}
