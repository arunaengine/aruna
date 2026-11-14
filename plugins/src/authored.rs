//! Reads the plugin.toml an author writes, with its short forms and defaults, into a manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use crate::connection::{Connection, Method};
use crate::manifest::{
    BuildStep, Manifest, ManifestError, Requests, SecretKind, SecretSlot, Setting, SettingKind,
};
use crate::permission::Permission;
use crate::trigger::{Trigger, TriggerKind};

/// The secret slot of the webhook's verifying secret.
const WEBHOOK_SLOT: &str = "webhook";

/// plugin.toml as an author writes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Authored {
    name: String,
    version: String,
    #[serde(default)]
    title: Option<String>,
    on: Vec<String>,
    #[serde(default)]
    access: Access,
    #[serde(default)]
    settings: BTreeMap<String, Field>,
    #[serde(default)]
    services: BTreeMap<String, Service>,
    #[serde(default)]
    secrets: BTreeMap<String, Secret>,
    #[serde(default)]
    timer: Option<Timer>,
    #[serde(default)]
    webhook: Option<Webhook>,
    #[serde(default)]
    manual: BTreeMap<String, Field>,
    #[serde(default)]
    build: Option<BuildStep>,
}

/// What the plugin may do with what the installer scopes: read only, or also write.
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Access {
    #[default]
    Read,
    Write,
}

/// A setting or form field: its title alone, which makes a text field, or a table.
#[derive(Deserialize)]
#[serde(untagged)]
enum Field {
    Title(String),
    Table(FieldTable),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldTable {
    #[serde(default)]
    title: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<SettingKind>,
    #[serde(default)]
    default: Option<Value>,
    #[serde(default)]
    choices: Vec<String>,
}

/// An outside address the plugin may call; every method by default.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Service {
    url: String,
    #[serde(default)]
    methods: Option<BTreeSet<Method>>,
    #[serde(default)]
    secret: Option<Secret>,
}

/// A secret: its title alone, which takes the slot's default kind, or a table.
#[derive(Deserialize)]
#[serde(untagged)]
enum Secret {
    Title(String),
    Table {
        title: String,
        #[serde(default)]
        kind: Option<SecretKind>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Timer {
    every: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Webhook {
    secret: Secret,
    #[serde(default)]
    delivery_id: Option<String>,
}

/// Reads plugin.toml text into a manifest; [`Manifest::check`] checks it afterwards.
pub fn parse(text: &str) -> Result<Manifest, ManifestError> {
    let authored: Authored =
        toml::from_str(text).map_err(|err| ManifestError::Malformed(err.to_string()))?;
    authored.manifest()
}

impl Authored {
    fn manifest(self) -> Result<Manifest, ManifestError> {
        let mut secrets = BTreeMap::new();
        let mut connections = BTreeMap::new();
        for (name, service) in self.services {
            let auth = match service.secret {
                Some(secret) => Some(slot(&mut secrets, &name, secret, SecretKind::Bearer)?),
                None => None,
            };
            let methods = service.methods.unwrap_or_else(all_methods);
            let connection = Connection {
                url: service.url,
                methods,
                auth,
                optional: false,
            };
            connections.insert(name, connection);
        }
        let readable = !self.secrets.is_empty();
        for (name, secret) in self.secrets {
            slot(&mut secrets, &name, secret, SecretKind::Bearer)?;
        }
        let mut triggers = BTreeMap::new();
        let (mut timer, mut webhook) = (self.timer, self.webhook);
        let mut manual = Some(self.manual);
        for name in &self.on {
            let kind = TriggerKind::named(name)
                .ok_or_else(|| malformed(format!("`on` lists `{name}`, which is no trigger")))?;
            let missing = |table: &str, key: &str| {
                malformed(format!(
                    "`on` lists `{name}`, but there is no `[{table}]` with `{key}`"
                ))
            };
            let trigger = match kind {
                TriggerKind::Every => Trigger {
                    interval: Some(timer.take().ok_or_else(|| missing("timer", "every"))?.every),
                    ..Trigger::default()
                },
                TriggerKind::Webhook => {
                    let hook = webhook.take().ok_or_else(|| missing("webhook", "secret"))?;
                    let verify = slot(&mut secrets, WEBHOOK_SLOT, hook.secret, SecretKind::Hmac)?;
                    Trigger {
                        verify: Some(verify),
                        delivery_id: hook.delivery_id,
                        ..Trigger::default()
                    }
                }
                TriggerKind::Manual => Trigger {
                    form: fields(manual.take().unwrap_or_default()),
                    ..Trigger::default()
                },
                _ => Trigger::default(),
            };
            if triggers.insert(kind, trigger).is_some() {
                return Err(malformed(format!("`on` lists `{name}` twice")));
            }
        }
        let unused = [
            (timer.is_some(), "timer"),
            (webhook.is_some(), "webhook"),
            (manual.is_some_and(|form| !form.is_empty()), "manual"),
        ];
        if let Some((_, table)) = unused.into_iter().find(|(set, _)| *set) {
            return Err(malformed(format!(
                "`[{table}]` is set, but `on` does not list `{table}`"
            )));
        }
        Ok(Manifest {
            abi: None,
            name: self.name,
            version: self.version,
            title: self.title,
            settings: fields(self.settings),
            secrets,
            connections,
            triggers,
            permissions: requests(self.access, readable),
            build: self.build,
        })
    }
}

/// Adds the secret slot `name`, refusing a second slot of the same name.
fn slot(
    slots: &mut BTreeMap<String, SecretSlot>,
    name: &str,
    secret: Secret,
    default: SecretKind,
) -> Result<String, ManifestError> {
    let (title, kind) = match secret {
        Secret::Title(title) => (title, default),
        Secret::Table { title, kind } => (title, kind.unwrap_or(default)),
    };
    if slots
        .insert(name.to_string(), SecretSlot { title, kind })
        .is_some()
    {
        return Err(malformed(format!("`{name}` names two secrets")));
    }
    Ok(name.to_string())
}

/// Settings or form fields with their short forms filled in: a text field, titled after its key.
fn fields(fields: BTreeMap<String, Field>) -> BTreeMap<String, Setting> {
    (fields.into_iter())
        .map(|(name, field)| {
            let table = match field {
                Field::Title(title) => FieldTable {
                    title: Some(title),
                    kind: None,
                    default: None,
                    choices: Vec::new(),
                },
                Field::Table(table) => table,
            };
            let setting = Setting {
                title: table.title.unwrap_or_else(|| words(&name)),
                kind: table.kind.unwrap_or(SettingKind::String),
                default: table.default,
                choices: table.choices,
            };
            (name, setting)
        })
        .collect()
}

/// The permissions an access asks for, until grants name read and write themselves.
fn requests(access: Access, readable: bool) -> Requests {
    let mut required = BTreeSet::from([Permission::MetadataRead, Permission::ObjectsRead]);
    if access == Access::Write {
        required.extend([Permission::MetadataWrite, Permission::MetadataCreate]);
    }
    if readable {
        required.insert(Permission::SecretsValue);
    }
    Requests {
        required,
        optional: BTreeSet::new(),
    }
}

fn all_methods() -> BTreeSet<Method> {
    use Method::{Delete, Get, Head, Patch, Post, Put};
    BTreeSet::from([Get, Head, Post, Put, Patch, Delete])
}

/// `chat_url` as a title: "Chat url".
fn words(name: &str) -> String {
    let spaced = name.replace('_', " ");
    let mut chars = spaced.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => spaced,
    }
}

fn malformed(message: String) -> ManifestError {
    ManifestError::Malformed(message)
}

#[cfg(test)]
#[path = "authored_tests.rs"]
mod tests;
