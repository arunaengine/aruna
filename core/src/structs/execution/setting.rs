//! Defines the replicated notification settings of a user and how the current one is chosen.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::document::DocumentTarget;
use crate::errors::ConversionError;
use crate::keyspaces::{EMAIL_SETTINGS_KEYSPACE, WEBHOOK_SETTINGS_KEYSPACE};
use crate::register::{RegisterStamp, current_write};
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
    /// Bound into sealed secrets, so the channels never mix.
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
                    stamp: setting.stamp,
                    on: setting.sealed.is_some(),
                }
            }
            Self::Webhook => {
                let setting = WebhookSubscription::from_bytes(bytes)?;
                SettingHead {
                    id: setting.subscription_id,
                    owner: setting.user_id,
                    stamp: setting.stamp,
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
    pub stamp: RegisterStamp,
    pub on: bool,
}

/// The current setting of one user and channel: the register write with the highest stamp.
/// An off setting wins only by its stamp. No setting means off.
pub fn current_setting<T>(
    settings: impl IntoIterator<Item = T>,
    head: impl Fn(&T) -> SettingHead,
) -> Option<T> {
    current_write(settings, |setting| head(setting).stamp)
}

/// The structural checks of a setting document, shared by sync admission and the management
/// nodes that open it. Returns the head of a valid document.
pub fn validate_setting(target: &DocumentTarget, bytes: &[u8]) -> Result<SettingHead, String> {
    let (channel, owner, id) =
        SettingChannel::of_target(target).ok_or("the target is not a notification setting")?;
    if owner.is_nil() || id.is_nil() {
        return Err("a setting needs an owner and a document id".to_string());
    }
    let head = channel
        .head(bytes)
        .map_err(|error| format!("undecodable setting: {error}"))?;
    if head.owner != owner || head.id != id || head.stamp.write_id != id {
        return Err("the setting does not match its target".to_string());
    }
    if head.stamp.counter == 0 {
        return Err("a setting counter starts at 1".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    fn head(counter: u64, node: u8, on: bool) -> SettingHead {
        let id = Ulid::from_bytes([counter as u8; 16]);
        SettingHead {
            id,
            owner: UserId::local(Ulid::from_bytes([1; 16]), RealmId::from_bytes([1; 32])),
            stamp: RegisterStamp {
                counter,
                node_id: iroh::SecretKey::from_bytes(&[node; 32]).public(),
                write_id: id,
            },
            on,
        }
    }

    fn winner(heads: &[SettingHead]) -> Option<SettingHead> {
        current_setting(heads.iter().copied(), |head| *head)
    }

    #[test]
    fn higher_counter_wins() {
        let heads = [head(1, 9, false), head(3, 1, true), head(2, 8, false)];
        assert_eq!(winner(&heads), Some(heads[1]));
        assert_eq!(winner(&[]), None, "no setting means off");
    }

    #[test]
    fn off_needs_stamp() {
        // An opt-out made before the latest opt-in loses; one made after it wins.
        let (on, off) = (head(3, 1, true), head(2, 2, false));
        assert_eq!(winner(&[off, on]), Some(on));
        let later = head(4, 2, false);
        assert_eq!(winner(&[on, later, off]), Some(later));
    }
}
