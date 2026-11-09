//! Defines the replicated notification settings of a user and how the current one is chosen.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::document::{DocumentChange, DocumentTarget};
use crate::errors::ConversionError;
use crate::keyspaces::{EMAIL_SETTINGS_KEYSPACE, WEBHOOK_SETTINGS_KEYSPACE};
use crate::structs::execution::email::{EMAIL_CATEGORIES, EmailPreference};
use crate::structs::execution::webhook::{WebhookSubscription, valid_url};

/// Bytes a sealed address or signing secret may take, with the HPKE overhead.
const SEALED_BYTES: usize = 1024;

/// The channel a notification setting turns on or off.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SettingChannel {
    Email,
    Webhook,
}

impl SettingChannel {
    /// Bound into sealed secrets and tentative row keys, so the channels never mix.
    pub const fn tag(self) -> u8 {
        match self {
            Self::Email => 1,
            Self::Webhook => 2,
        }
    }

    pub const fn keyspace(self) -> &'static str {
        match self {
            Self::Email => EMAIL_SETTINGS_KEYSPACE,
            Self::Webhook => WEBHOOK_SETTINGS_KEYSPACE,
        }
    }

    pub fn target(self, owner: UserId, id: Ulid) -> DocumentTarget {
        match self {
            Self::Email => DocumentTarget::EmailPreference {
                owner,
                preference_id: id,
            },
            Self::Webhook => DocumentTarget::WebhookSubscription {
                owner,
                subscription_id: id,
            },
        }
    }

    /// The channel, owner and document id of a setting target.
    pub fn of_target(target: &DocumentTarget) -> Option<(Self, UserId, Ulid)> {
        match target {
            DocumentTarget::EmailPreference {
                owner,
                preference_id,
            } => Some((Self::Email, *owner, *preference_id)),
            DocumentTarget::WebhookSubscription {
                owner,
                subscription_id,
            } => Some((Self::Webhook, *owner, *subscription_id)),
            _ => None,
        }
    }

    /// The sealed secret of a stored setting of this channel; `None` when it is off.
    pub fn sealed(self, bytes: &[u8]) -> Result<Option<Vec<u8>>, ConversionError> {
        Ok(match self {
            Self::Email => EmailPreference::from_bytes(bytes)?.sealed,
            Self::Webhook => WebhookSubscription::from_bytes(bytes)?.sealed,
        })
    }

    /// Decodes a stored setting of this channel to the fields that order it.
    pub fn head(self, bytes: &[u8]) -> Result<SettingHead, ConversionError> {
        Ok(match self {
            Self::Email => {
                let setting = EmailPreference::from_bytes(bytes)?;
                SettingHead {
                    id: setting.preference_id,
                    owner: setting.user_id,
                    version: setting.version,
                    on: setting.sealed.is_some(),
                }
            }
            Self::Webhook => {
                let setting = WebhookSubscription::from_bytes(bytes)?;
                SettingHead {
                    id: setting.subscription_id,
                    owner: setting.user_id,
                    version: setting.version,
                    on: setting.sealed.is_some(),
                }
            }
        })
    }
}

/// The fields of one setting document that decide between the documents of a user.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettingHead {
    pub id: Ulid,
    pub owner: UserId,
    pub version: u64,
    pub on: bool,
}

/// The winner among the confirmed settings of one user and channel: the highest version, then
/// off, then the larger document id. No setting means off.
pub fn current_setting<T>(
    settings: impl IntoIterator<Item = T>,
    head: impl Fn(&T) -> SettingHead,
) -> Option<T> {
    settings.into_iter().max_by_key(|setting| {
        let head = head(setting);
        (head.version, !head.on, head.id)
    })
}

/// More than half of the `managers` management nodes of the realm config.
pub const fn quorum_size(managers: usize) -> usize {
    managers / 2 + 1
}

/// The structural checks of a setting document, shared by sync admission and the management
/// nodes that store it. Returns the head of a valid document.
pub fn validate_setting(target: &DocumentTarget, bytes: &[u8]) -> Result<SettingHead, String> {
    let (channel, owner, id) =
        SettingChannel::of_target(target).ok_or("the target is not a notification setting")?;
    if owner.is_nil() || id.is_nil() {
        return Err("a setting needs an owner and a document id".to_string());
    }
    let head = channel
        .head(bytes)
        .map_err(|error| format!("undecodable setting: {error}"))?;
    if head.owner != owner || head.id != id {
        return Err("the setting does not match its target".to_string());
    }
    if head.version == 0 {
        return Err("a setting version starts at 1".to_string());
    }
    let shaped = match channel {
        SettingChannel::Email => {
            let setting = EmailPreference::from_bytes(bytes).map_err(|error| error.to_string())?;
            let known = (setting.categories.iter())
                .all(|category| EMAIL_CATEGORIES.contains(&category.as_str()));
            known && setting.categories.is_empty() != head.on && sealed_fits(&setting.sealed)
        }
        SettingChannel::Webhook => {
            let setting =
                WebhookSubscription::from_bytes(bytes).map_err(|error| error.to_string())?;
            let url = setting.url.as_deref();
            url.is_none_or(valid_url) && url.is_some() == head.on && sealed_fits(&setting.sealed)
        }
    };
    match shaped {
        true => Ok(head),
        false => Err("the setting fields do not match its on or off state".to_string()),
    }
}

fn sealed_fits(sealed: &Option<Vec<u8>>) -> bool {
    sealed
        .as_ref()
        .is_none_or(|sealed| sealed.len() <= SEALED_BYTES)
}

/// Row key of a setting document: the owner, then the document id, so one prefix lists a user.
pub fn setting_key(owner: &UserId, id: Ulid) -> Vec<u8> {
    [owner.to_bytes().as_slice(), &id.to_bytes()].concat()
}

/// Row key of a tentative setting: the channel tag, the owner, then the document id.
pub fn tentative_key(channel: SettingChannel, owner: &UserId, id: Ulid) -> Vec<u8> {
    [&[channel.tag()][..], &owner.to_bytes(), &id.to_bytes()].concat()
}

pub fn tentative_prefix(channel: SettingChannel, owner: &UserId) -> Vec<u8> {
    [&[channel.tag()][..], &owner.to_bytes()].concat()
}

/// A setting a management node stored but no quorum confirmed yet; it only reserves a version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TentativeSetting {
    pub bytes: Vec<u8>,
    pub change: DocumentChange,
}

impl TentativeSetting {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    fn head(id: u8, version: u64, on: bool) -> SettingHead {
        SettingHead {
            id: Ulid::from_bytes([id; 16]),
            owner: UserId::local(Ulid::from_bytes([1; 16]), RealmId::from_bytes([1; 32])),
            version,
            on,
        }
    }

    fn winner(heads: &[SettingHead]) -> Option<SettingHead> {
        current_setting(heads.iter().copied(), |head| *head)
    }

    #[test]
    fn higher_version_wins() {
        let heads = [head(9, 1, false), head(1, 3, true), head(8, 2, false)];
        assert_eq!(winner(&heads), Some(heads[1]));
        assert_eq!(winner(&[]), None, "no setting means off");
    }

    #[test]
    fn equal_version_off() {
        let heads = [head(9, 2, true), head(1, 2, false)];
        assert_eq!(winner(&heads), Some(heads[1]));
    }

    #[test]
    fn equal_version_larger() {
        let heads = [head(1, 2, true), head(9, 2, true), head(5, 2, true)];
        assert_eq!(winner(&heads), Some(heads[1]));
    }

    #[test]
    fn quorum_size_counts() {
        let sizes: Vec<_> = (0..=5).map(quorum_size).collect();
        assert_eq!(sizes, [1, 1, 2, 2, 3, 3]);
    }
}
