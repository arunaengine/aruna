//! Reads the plugin.toml an author writes, with its short forms and defaults, into a manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use aruna_plugin_contract::connection::{Connection, Method};
use aruna_plugin_contract::manifest::{
    Manifest, ManifestError, Requests, SecretKind, SecretSlot, Setting, SettingKind,
};
use aruna_plugin_contract::permission::Permission;
use aruna_plugin_contract::recipe::{DEFAULT_PATTERN, Network, Recipe, placeholders};
use aruna_plugin_contract::trigger::{Trigger, TriggerKind};

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
    tasks: BTreeMap<String, Task>,
    #[serde(default)]
    timer: Option<Timer>,
    #[serde(default)]
    webhook: Option<Webhook>,
    #[serde(default)]
    manual: BTreeMap<String, Field>,
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
#[serde(
    untagged,
    expecting = "a secret title, or a table with `title` and `kind`"
)]
enum Secret {
    Title(String),
    Table(SecretTable),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretTable {
    title: String,
    #[serde(default)]
    kind: Option<SecretKind>,
}

/// A container task: a pinned image and, unless its entrypoint fits, a command; the rest has
/// defaults.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Task {
    image: String,
    #[serde(default)]
    command: Option<Vec<String>>,
    #[serde(default = "one")]
    cpu: u32,
    #[serde(default = "two_gib")]
    memory: String,
    #[serde(default = "one_hour")]
    time: String,
    #[serde(default)]
    network: Network,
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    #[serde(default)]
    report: Option<String>,
    /// Refused until containers can get secrets; read so the author learns why.
    #[serde(default)]
    secrets: Option<toml::Value>,
}

fn one() -> u32 {
    1
}

fn two_gib() -> String {
    "2 GiB".to_string()
}

fn one_hour() -> String {
    "1h".to_string()
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
            if let Some(slot) = secrets.get_mut(&name) {
                slot.readable = true;
            }
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
        let mut recipes = BTreeMap::new();
        for (name, task) in self.tasks {
            if self.access != Access::Write {
                return Err(malformed(format!(
                    "task `{name}` writes its outputs, so the plugin needs `access = \"write\"`"
                )));
            }
            recipes.insert(name.clone(), task.recipe(&name)?);
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
            permissions: requests(self.access, readable, !recipes.is_empty()),
            recipes,
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
        Secret::Table(table) => (table.title, table.kind.unwrap_or(default)),
    };
    let slot = SecretSlot {
        title,
        kind,
        readable: false,
    };
    if slots.insert(name.to_string(), slot).is_some() {
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

impl Task {
    /// The task with its defaults filled in: every `{name}` without a pattern gets the default.
    fn recipe(self, name: &str) -> Result<Recipe, ManifestError> {
        let invalid = |why: &str| malformed(format!("task `{name}`: {why}"));
        if self.secrets.is_some() {
            return Err(invalid(
                "secrets in the container need a later Aruna version",
            ));
        }
        let used = placeholders(self.command.as_deref().unwrap_or_default());
        let used = used.map_err(invalid)?;
        if let Some(unused) = self.parameters.keys().find(|key| !used.contains(*key)) {
            return Err(invalid(&format!(
                "`{unused}` is no `{{name}}` in the command"
            )));
        }
        let mut parameters = self.parameters;
        for name in used {
            parameters
                .entry(name)
                .or_insert_with(|| DEFAULT_PATTERN.to_string());
        }
        let memory = bytes(&self.memory)
            .ok_or_else(|| invalid("memory is a whole number of MiB or GiB, such as \"2 GiB\""))?;
        let time = seconds(&self.time)
            .ok_or_else(|| invalid("time is a whole number of m, h or d, such as \"1h\""))?;
        Ok(Recipe {
            image: self.image,
            command: self.command,
            cpu: self.cpu,
            memory,
            time,
            network: self.network,
            parameters,
            report: self.report,
        })
    }
}

/// `512 MiB` or `2 GiB` in bytes.
fn bytes(text: &str) -> Option<u64> {
    let text = text.trim();
    let (count, unit) = text.split_at(text.find(|c: char| !c.is_ascii_digit())?);
    let unit = match unit.trim() {
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    count.parse::<u64>().ok()?.checked_mul(unit)
}

/// `30m`, `1h` or `2d` in seconds.
fn seconds(text: &str) -> Option<u64> {
    let at = (text.len().checked_sub(1)).filter(|at| text.is_char_boundary(*at))?;
    let (count, unit) = text.split_at(at);
    let unit = match unit {
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => return None,
    };
    count.parse::<u64>().ok()?.checked_mul(unit)
}

/// The permissions an access asks for, until grants name read and write themselves.
fn requests(access: Access, readable: bool, tasks: bool) -> Requests {
    // An upload reads its source and sends it through a declared service, as `send` may.
    let mut required = BTreeSet::from([
        Permission::MetadataRead,
        Permission::ObjectsRead,
        Permission::TransfersUpload,
    ]);
    if access == Access::Write {
        // A copy needs read on the source and write on the target; a download writes its target.
        required.extend([
            Permission::MetadataWrite,
            Permission::MetadataCreate,
            Permission::ObjectsCopy,
            Permission::TransfersDownload,
        ]);
    }
    if readable {
        required.insert(Permission::SecretsValue);
    }
    if tasks {
        required.insert(Permission::JobsRun);
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
