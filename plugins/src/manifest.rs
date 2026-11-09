//! Declares what a plugin asks for: settings, secret slots, connections, triggers, permissions.
//! A manifest only asks; it never grants. `build` wires triggers to handlers before publishing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::connection::{Address, Connection};
use crate::permission::{Import, Permission};
use crate::trigger::{Trigger, TriggerKind};

/// Host interface package every artifact manifest must name.
pub const PLUGIN_ABI: &str = "aruna:plugin@0.2.0";
pub const MAX_MANIFEST: usize = 64 * 1024;
pub const MAX_ENTRIES: usize = 32;
pub const MAX_NAME: usize = 64;
pub const MAX_TITLE: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SettingKind {
    String,
    Integer,
    Boolean,
    Url,
    Choice,
}

/// One value the installer fills in. The form is generated from these.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setting {
    pub title: String,
    #[serde(rename = "type")]
    pub kind: SettingKind,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub choices: Vec<String>,
}

/// Vault kinds a slot may bind to; the host injects or verifies, the code never reads them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretKind {
    #[default]
    Bearer,
    Basic,
    Header,
    Hmac,
}

impl SecretKind {
    pub fn injects(self) -> bool {
        !matches!(self, Self::Hmac)
    }

    pub fn verifies(self) -> bool {
        !matches!(self, Self::Basic)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretSlot {
    pub title: String,
    #[serde(default)]
    pub kind: SecretKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requests {
    #[serde(default)]
    pub required: BTreeSet<Permission>,
    #[serde(default)]
    pub optional: BTreeSet<Permission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// Set by `build`; an authored manifest leaves it out.
    #[serde(default)]
    pub abi: Option<String>,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub settings: BTreeMap<String, Setting>,
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretSlot>,
    #[serde(default)]
    pub connections: BTreeMap<String, Connection>,
    #[serde(default)]
    pub triggers: BTreeMap<TriggerKind, Trigger>,
    #[serde(default)]
    pub permissions: Requests,
    /// Handler functions the code exports; set by `build`.
    #[serde(default)]
    pub handlers: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ManifestError {
    #[error("manifest is not valid: {0}")]
    Malformed(String),
    #[error("manifest exceeds {MAX_MANIFEST} bytes")]
    TooLarge,
    #[error("manifest names host interface `{0}`, expected {PLUGIN_ABI}")]
    UnsupportedAbi(String),
    #[error("`{0}` is not a valid name")]
    InvalidName(String),
    #[error("`{0}` is not a version like 1.2.3")]
    InvalidVersion(String),
    #[error("a title must have 1 to {MAX_TITLE} characters")]
    InvalidTitle,
    #[error("more than {MAX_ENTRIES} {0}")]
    TooMany(&'static str),
    #[error("setting `{0}`: {1}")]
    InvalidSetting(String, &'static str),
    #[error("connection `{0}`: {1}")]
    InvalidConnection(String, &'static str),
    #[error("trigger `{kind}`: {why}", kind = .0.as_str(), why = .1)]
    InvalidTrigger(TriggerKind, &'static str),
    #[error("`{0}` names no secret slot of a fitting kind")]
    InvalidSlot(String),
    #[error("trigger `{kind}` needs permission `{needs}`", kind = .0.as_str(), needs = .1)]
    MissingPermission(TriggerKind, Permission),
    #[error("permission `{0}` is listed twice or cannot be requested")]
    InvalidPermission(Permission),
    #[error("trigger `{}` has no handler to call", .0.as_str())]
    NoHandler(TriggerKind),
    #[error("handler `{0}` is not exported")]
    UnknownHandler(String),
    #[error("handler `{0}` is exported but no trigger calls it")]
    UnusedHandler(String),
    #[error("the code imports `{0}`, which the manifest does not request")]
    UndeclaredImport(String),
    #[error("the code imports `{0}`, which the plugin host does not provide")]
    UnknownImport(String),
}

impl Manifest {
    /// Parses the manifest embedded in an artifact: checked and fully wired.
    pub fn parse(bytes: &[u8]) -> Result<Self, ManifestError> {
        if bytes.len() > MAX_MANIFEST {
            return Err(ManifestError::TooLarge);
        }
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|error| ManifestError::Malformed(error.to_string()))?;
        match manifest.abi.as_deref() {
            Some(PLUGIN_ABI) => {}
            other => {
                return Err(ManifestError::UnsupportedAbi(
                    other.unwrap_or_default().to_string(),
                ));
            }
        }
        manifest.check()?;
        manifest.check_wiring()?;
        Ok(manifest)
    }

    /// Checks everything that does not depend on the compiled code.
    pub fn check(&self) -> Result<(), ManifestError> {
        if !valid_plugin(&self.name) {
            return Err(ManifestError::InvalidName(self.name.clone()));
        }
        if !valid_version(&self.version) {
            return Err(ManifestError::InvalidVersion(self.version.clone()));
        }
        if self
            .title
            .as_deref()
            .is_some_and(|title| !valid_title(title))
        {
            return Err(ManifestError::InvalidTitle);
        }
        bounded("settings", self.settings.len())?;
        bounded("secret slots", self.secrets.len())?;
        bounded("connections", self.connections.len())?;
        bounded("handlers", self.handlers.len())?;
        let names = self.settings.keys().chain(self.secrets.keys());
        let names = names.chain(self.connections.keys()).chain(&self.handlers);
        if let Some(name) = names.into_iter().find(|name| !valid_name(name)) {
            return Err(ManifestError::InvalidName(name.clone()));
        }
        for (name, setting) in &self.settings {
            check_setting(setting)
                .map_err(|why| ManifestError::InvalidSetting(name.clone(), why))?;
        }
        if self.secrets.values().any(|slot| !valid_title(&slot.title)) {
            return Err(ManifestError::InvalidTitle);
        }
        for (name, connection) in &self.connections {
            self.check_connection(name, connection)?;
        }
        for (kind, trigger) in &self.triggers {
            self.check_trigger(*kind, trigger)?;
        }
        self.check_permissions()
    }

    /// Resolves every trigger's handler against the exported `handlers` and marks the result
    /// as built for this host. Fails on a missing, unknown or unused handler.
    pub fn wire(&mut self, handlers: BTreeSet<String>) -> Result<(), ManifestError> {
        self.handlers = handlers;
        let only = match self.handlers.len() {
            1 => self.handlers.first().cloned(),
            _ => None,
        };
        for (kind, trigger) in &mut self.triggers {
            if trigger.handler.is_none() {
                let named = self.handlers.contains(kind.as_str());
                let handler = named.then(|| kind.as_str().to_string()).or(only.clone());
                trigger.handler = Some(handler.ok_or(ManifestError::NoHandler(*kind))?);
            }
        }
        self.abi = Some(PLUGIN_ABI.to_string());
        self.check()?;
        self.check_wiring()
    }

    /// Checks that the code imports only host interfaces this manifest requests.
    pub fn check_imports<'a>(
        &self,
        imports: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), ManifestError> {
        let (package, version) = PLUGIN_ABI.split_once('@').unwrap_or((PLUGIN_ABI, ""));
        for import in imports {
            let interface = import
                .strip_prefix(package)
                .and_then(|rest| rest.strip_prefix('/'))
                .and_then(|rest| rest.strip_suffix(version))
                .and_then(|rest| rest.strip_suffix('@'));
            let needed = interface.and_then(Import::of);
            let allowed = match needed {
                None => return Err(ManifestError::UnknownImport(import.to_string())),
                Some(Import::Free) => true,
                Some(Import::Needs(permission)) => self.requested().contains(&permission),
                Some(Import::Connections) => !self.connections.is_empty(),
            };
            if !allowed {
                return Err(ManifestError::UndeclaredImport(import.to_string()));
            }
        }
        Ok(())
    }

    /// Required and optional permissions together: the upper bound of any grant.
    pub fn requested(&self) -> BTreeSet<Permission> {
        let requests = &self.permissions;
        requests
            .required
            .union(&requests.optional)
            .copied()
            .collect()
    }

    fn check_wiring(&self) -> Result<(), ManifestError> {
        let mut used = BTreeSet::new();
        for (kind, trigger) in &self.triggers {
            let handler = trigger
                .handler
                .as_ref()
                .ok_or(ManifestError::NoHandler(*kind))?;
            if !self.handlers.contains(handler) {
                return Err(ManifestError::UnknownHandler(handler.clone()));
            }
            used.insert(handler);
        }
        match self.handlers.iter().find(|handler| !used.contains(handler)) {
            Some(unused) => Err(ManifestError::UnusedHandler(unused.clone())),
            None => Ok(()),
        }
    }

    fn check_connection(&self, name: &str, connection: &Connection) -> Result<(), ManifestError> {
        let invalid = |why| ManifestError::InvalidConnection(name.to_string(), why);
        connection.check_fields().map_err(invalid)?;
        if let Address::Setting { setting, .. } = connection.address().map_err(invalid)? {
            let kind = self.settings.get(setting).map(|setting| setting.kind);
            if kind != Some(SettingKind::Url) {
                return Err(invalid("the address must start with a url setting"));
            }
        }
        match &connection.auth {
            Some(slot) if !self.slot_is(slot, SecretKind::injects) => {
                Err(ManifestError::InvalidSlot(slot.clone()))
            }
            _ => Ok(()),
        }
    }

    fn check_trigger(&self, kind: TriggerKind, trigger: &Trigger) -> Result<(), ManifestError> {
        let invalid = |why| ManifestError::InvalidTrigger(kind, why);
        if let Some(handler) = &trigger.handler
            && !valid_name(handler)
        {
            return Err(ManifestError::InvalidName(handler.clone()));
        }
        let timer = kind == TriggerKind::Every;
        let webhook = kind == TriggerKind::Webhook;
        if trigger.input.is_some() && kind != TriggerKind::Manual {
            return Err(invalid("only a manual trigger takes input"));
        }
        if timer != trigger.interval.is_some() || (timer && trigger.interval_secs().is_none()) {
            return Err(invalid(
                "`every` needs an interval from 1m to 366d, and only it has one",
            ));
        }
        if !webhook && (trigger.verify.is_some() || trigger.delivery_id.is_some()) {
            return Err(invalid("only a webhook trigger verifies requests"));
        }
        if webhook {
            let slot = trigger
                .verify
                .as_ref()
                .ok_or(invalid("a webhook needs a verify slot"))?;
            if !self.slot_is(slot, SecretKind::verifies) {
                return Err(ManifestError::InvalidSlot(slot.clone()));
            }
            if trigger
                .delivery_id
                .as_deref()
                .is_some_and(|name| !valid_header(name))
            {
                return Err(invalid("the delivery id must be a lowercase header name"));
            }
        }
        match kind.needs() {
            Some(permission) if !self.requested().contains(&permission) => {
                Err(ManifestError::MissingPermission(kind, permission))
            }
            _ => Ok(()),
        }
    }

    fn check_permissions(&self) -> Result<(), ManifestError> {
        let requests = &self.permissions;
        let both = requests.required.intersection(&requests.optional);
        let generated = self.requested().into_iter().filter(|p| p.generated());
        match both.copied().chain(generated).next() {
            Some(permission) => Err(ManifestError::InvalidPermission(permission)),
            None => Ok(()),
        }
    }

    fn slot_is(&self, slot: &str, fits: fn(SecretKind) -> bool) -> bool {
        self.secrets.get(slot).is_some_and(|slot| fits(slot.kind))
    }
}

fn check_setting(setting: &Setting) -> Result<(), &'static str> {
    if !valid_title(&setting.title) {
        return Err("the title must have 1 to 128 characters");
    }
    let choice = setting.kind == SettingKind::Choice;
    if choice == setting.choices.is_empty() || setting.choices.len() > MAX_ENTRIES {
        return Err("a choice needs 1 to 32 choices, and only a choice has them");
    }
    let Some(default) = &setting.default else {
        return Ok(());
    };
    let fits = match setting.kind {
        SettingKind::String => default.is_string(),
        SettingKind::Integer => default.is_i64(),
        SettingKind::Boolean => default.is_boolean(),
        SettingKind::Url => default.as_str().is_some_and(https_origin),
        SettingKind::Choice => default
            .as_str()
            .is_some_and(|d| setting.choices.iter().any(|c| c == d)),
    };
    if !fits {
        return Err("the default does not fit the type");
    }
    Ok(())
}

/// Whether `value` is an https origin with an optional path, as a url setting must be.
pub fn https_origin(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

/// A plugin name: 1 to 64 of `a-z`, `0-9` or `-`, starting with a letter, no `-` at the end.
pub fn valid_plugin(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=MAX_NAME).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && !name.ends_with('-')
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// A setting, slot, connection or handler name: 1 to 64 of `a-z`, `0-9` or `_`, starting with a letter.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=MAX_NAME).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// A lowercase header name of at most 64 token characters.
pub fn valid_header(name: &str) -> bool {
    (1..=MAX_NAME).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_.".contains(&b))
}

fn valid_title(title: &str) -> bool {
    let count = title.chars().count();
    (1..=MAX_TITLE).contains(&count) && !title.chars().any(char::is_control)
}

/// `major.minor.patch` with an optional `-` or `+` suffix of plain characters.
fn valid_version(version: &str) -> bool {
    let end = version.find(['-', '+']).unwrap_or(version.len());
    let (core, suffix) = version.split_at(end);
    let numbers: Vec<&str> = core.split('.').collect();
    let number = |part: &&str| {
        !part.is_empty() && part.len() <= 9 && part.bytes().all(|b| b.is_ascii_digit())
    };
    let plain = |b: u8| b.is_ascii_alphanumeric() || b"-+.".contains(&b);
    numbers.len() == 3
        && numbers.iter().all(number)
        && version.len() <= MAX_NAME
        && suffix.bytes().all(plain)
        && suffix.len() != 1
}

fn bounded(what: &'static str, count: usize) -> Result<(), ManifestError> {
    match count > MAX_ENTRIES {
        true => Err(ManifestError::TooMany(what)),
        false => Ok(()),
    }
}

