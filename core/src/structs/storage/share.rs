//! Share links: node-local records that let visitors without an account act for the link's
//! creator, narrowed to one bucket, folder, object or dataset and one access mode.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Both keys are sealed at rest with the record; tokens and tickets are never logged.

use std::net::IpAddr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use ulid::Ulid;

use crate::UserId;
use crate::errors::ConversionError;
use crate::password::PasswordHash;
use crate::types::GroupId;

/// The `Authorization` scheme visitors send link and visit tokens in.
pub const SHARE_SCHEME: &str = "Share";
/// The longest a visit token lasts; never past the link's own expiry.
pub const VISIT_SECS: u64 = 60 * 60;
/// The longest a download ticket lasts.
pub const TICKET_SECS: u64 = 60;
const VISIT_DOMAIN: &[u8] = b"aruna share visit v1";
const TICKET_DOMAIN: &[u8] = b"aruna share ticket v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareTarget {
    Bucket {
        bucket: String,
    },
    /// A key prefix that ends with `/`.
    Folder {
        bucket: String,
        prefix: String,
    },
    Object {
        bucket: String,
        key: String,
    },
    /// A metadata dataset and the files it names on this node.
    Dataset {
        document_id: Ulid,
    },
}

impl ShareTarget {
    pub fn bucket(&self) -> Option<&str> {
        match self {
            Self::Bucket { bucket } | Self::Folder { bucket, .. } | Self::Object { bucket, .. } => {
                Some(bucket)
            }
            Self::Dataset { .. } => None,
        }
    }

    /// The key prefix every object of a bucket or folder target starts with.
    pub fn prefix(&self) -> Option<&str> {
        match self {
            Self::Bucket { .. } => Some(""),
            Self::Folder { prefix, .. } => Some(prefix),
            Self::Object { .. } | Self::Dataset { .. } => None,
        }
    }

    /// Whether `key` of `bucket` lies inside a bucket, folder or object target.
    pub fn covers(&self, bucket: &str, key: &str) -> bool {
        match self {
            Self::Bucket { bucket: own } => own == bucket,
            Self::Folder {
                bucket: own,
                prefix,
            } => own == bucket && key.starts_with(prefix),
            Self::Object {
                bucket: own,
                key: own_key,
            } => own == bucket && own_key == key,
            Self::Dataset { .. } => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareMode {
    Read,
    ReadUpload,
    /// A file drop: visitors upload and see nothing that is stored.
    Upload,
}

impl ShareMode {
    pub fn reads(self) -> bool {
        matches!(self, Self::Read | Self::ReadUpload)
    }

    pub fn uploads(self) -> bool {
        matches!(self, Self::ReadUpload | Self::Upload)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareLink {
    pub id: Ulid,
    pub group_id: GroupId,
    pub target: ShareTarget,
    pub mode: ShareMode,
    pub created_by: UserId,
    /// Unix seconds.
    pub created_at: u64,
    pub label: String,
    /// Unix seconds after which the link no longer opens.
    pub expires_at: Option<u64>,
    pub password: Option<PasswordHash>,
    /// Disclosed in the link URL; it only opens the link.
    pub secret: [u8; 32],
    /// Never leaves the node; it signs visit tokens and download tickets.
    pub signing_key: [u8; 32],
    pub revoked_at: Option<u64>,
}

impl std::fmt::Debug for ShareLink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareLink")
            .field("id", &self.id)
            .field("target", &self.target)
            .field("mode", &self.mode)
            .field("created_by", &self.created_by)
            .field("expires_at", &self.expires_at)
            .field("revoked_at", &self.revoked_at)
            .finish_non_exhaustive()
    }
}

impl ShareLink {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Not revoked and not past its expiry at `now` (Unix seconds).
    pub fn active(&self, now: u64) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|expiry| now < expiry)
    }

    /// The token the link URL carries in its fragment: `<id>.<secret>`.
    pub fn link_token(&self) -> String {
        format!("{}.{}", self.id, URL_SAFE_NO_PAD.encode(self.secret))
    }

    /// A visit token for one browser session, ending at `expires_at` (Unix seconds).
    pub fn visit_token(&self, expires_at: u64) -> String {
        let mac = self.mac(VISIT_DOMAIN, &expires_at.to_be_bytes());
        format!("v.{}.{expires_at}.{}", self.id, URL_SAFE_NO_PAD.encode(mac))
    }

    /// Signs a single-use download ticket for one object.
    pub fn ticket(&self, body: &TicketBody) -> Result<String, ConversionError> {
        let bytes = postcard::to_allocvec(body)?;
        let mac = self.mac(TICKET_DOMAIN, &bytes);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(mac)
        ))
    }

    /// Whether `token` opens this link at `now`; checks the secret or the visit signature.
    pub fn admits(&self, token: &ShareToken, now: u64) -> bool {
        match token {
            ShareToken::Link { id, secret } => {
                *id == self.id && bool::from(secret.as_slice().ct_eq(&self.secret))
            }
            ShareToken::Visit {
                id,
                expires_at,
                mac,
            } => {
                let expected = self.mac(VISIT_DOMAIN, &expires_at.to_be_bytes());
                *id == self.id
                    && now < *expires_at
                    && *expires_at <= now.saturating_add(VISIT_SECS)
                    && bool::from(mac.as_slice().ct_eq(&expected))
            }
        }
    }

    /// The ticket body when `ticket` was signed by this link and is still valid at `now`.
    pub fn open_ticket(&self, ticket: &str, now: u64) -> Option<TicketBody> {
        let (body, mac) = ticket.split_once('.')?;
        let bytes = URL_SAFE_NO_PAD.decode(body).ok()?;
        let mac = URL_SAFE_NO_PAD.decode(mac).ok()?;
        let expected = self.mac(TICKET_DOMAIN, &bytes);
        if !bool::from(mac.as_slice().ct_eq(&expected)) {
            return None;
        }
        let body: TicketBody = postcard::from_bytes(&bytes).ok()?;
        (body.share_id == self.id
            && now < body.expires_at
            && body.expires_at <= now.saturating_add(TICKET_SECS))
        .then_some(body)
    }

    fn mac(&self, domain: &[u8], payload: &[u8]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_keyed(&self.signing_key);
        hasher.update(domain);
        hasher.update(&self.id.to_bytes());
        hasher.update(payload);
        *hasher.finalize().as_bytes()
    }
}

/// What a download ticket allows: one object version, once, for a few seconds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketBody {
    pub share_id: Ulid,
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub expires_at: u64,
    pub nonce: [u8; 16],
}

/// A token from an `Authorization: Share` header.
#[derive(Clone, PartialEq, Eq)]
pub enum ShareToken {
    Link {
        id: Ulid,
        secret: [u8; 32],
    },
    Visit {
        id: Ulid,
        expires_at: u64,
        mac: [u8; 32],
    },
}

impl std::fmt::Debug for ShareToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ShareToken(redacted)")
    }
}

impl ShareToken {
    pub fn parse(text: &str) -> Option<Self> {
        let decode =
            |text: &str| -> Option<[u8; 32]> { URL_SAFE_NO_PAD.decode(text).ok()?.try_into().ok() };
        let parts: Vec<&str> = text.split('.').collect();
        match parts.as_slice() {
            [id, secret] => Some(Self::Link {
                id: Ulid::from_string(id).ok()?,
                secret: decode(secret)?,
            }),
            ["v", id, expires_at, mac] => Some(Self::Visit {
                id: Ulid::from_string(id).ok()?,
                expires_at: expires_at.parse().ok()?,
                mac: decode(mac)?,
            }),
            _ => None,
        }
    }

    pub fn share_id(&self) -> Ulid {
        match self {
            Self::Link { id, .. } | Self::Visit { id, .. } => *id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareAction {
    Open,
    WrongPassword,
    Download,
    Upload,
}

/// One entry of a link's access log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareEvent {
    /// Unix milliseconds.
    pub at: u64,
    pub action: ShareAction,
    pub key: Option<String>,
    pub bytes: Option<u64>,
    /// The visitor's network: an IPv4 /24 or an IPv6 /48.
    pub client: Option<String>,
}

impl ShareEvent {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Owner index row: the creator's storage key, then the link id.
pub fn owner_key(user: &UserId, share_id: Ulid) -> Vec<u8> {
    [user.to_storage_key(), share_id.to_bytes().to_vec()].concat()
}

/// Access log row: the link id, then a time-ordered entry id.
pub fn event_key(share_id: Ulid, entry_id: Ulid) -> Vec<u8> {
    [share_id.to_bytes(), entry_id.to_bytes()].concat()
}

/// The client network kept in the log, never the full address.
pub fn client_network(address: IpAddr) -> String {
    match address {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            format!("{a}.{b}.{c}.0/24")
        }
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => client_network(IpAddr::V4(v4)),
            None => {
                let segments = v6.segments();
                format!("{:x}:{:x}:{:x}::/48", segments[0], segments[1], segments[2])
            }
        },
    }
}

#[cfg(test)]
#[path = "share_tests.rs"]
mod tests;
