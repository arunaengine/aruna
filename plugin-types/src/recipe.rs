//! Declares the container tasks a plugin may start and checks them: a pinned image, a fixed
//! argument list whose `{name}` parts the code fills, resources, network and a report path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A parameter value without its own pattern: a letter or digit, then up to 127 letters, digits
/// or `._:-`, so a value can never start with `-` and be read as an option.
pub const DEFAULT_PATTERN: &str = "[A-Za-z0-9][A-Za-z0-9._:-]{0,127}";
pub const MAX_ARGUMENTS: usize = 64;
pub const MAX_ARGUMENT: usize = 4096;
pub const MAX_IMAGE: usize = 512;
pub const MAX_PATH: usize = 1024;
pub const MAX_PATTERN: usize = 256;

/// Whether the container reaches any network.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    #[default]
    None,
    Open,
}

/// One container task. The node fills the command, nothing else comes from the code.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recipe {
    /// An image reference pinned by `@sha256:` digest.
    pub image: String,
    /// Replaces the image's entrypoint and arguments when set.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    pub cpu: u32,
    /// Memory ceiling in bytes.
    pub memory: u64,
    /// Walltime ceiling in seconds.
    pub time: u64,
    #[serde(default)]
    pub network: Network,
    /// Pattern of every `{name}` in the command; a value must match it whole.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// One container file the node reads into the job's report; never exported.
    #[serde(default)]
    pub report: Option<String>,
}

impl Recipe {
    /// Checks everything that does not depend on the realm: the pinned image, the placeholders
    /// against the parameters, the patterns, the resources and the report path.
    pub fn check(&self) -> Result<(), &'static str> {
        if !pinned(&self.image) {
            return Err("the image must be pinned by `@sha256:` digest");
        }
        let command = self.command.as_deref().unwrap_or_default();
        if command.len() > MAX_ARGUMENTS
            || command
                .iter()
                .any(|part| part.len() > MAX_ARGUMENT || part.contains('\0'))
        {
            return Err("the command has at most 64 arguments of at most 4096 bytes");
        }
        let used = placeholders(command)?;
        if used.iter().ne(self.parameters.keys()) {
            return Err("every `{name}` in the command needs a pattern, and only those");
        }
        if self
            .parameters
            .values()
            .any(|pattern| matcher(pattern).is_none())
        {
            return Err("a parameter pattern is not a valid regular expression");
        }
        if self.cpu == 0 || self.memory == 0 || self.time == 0 {
            return Err("cpu, memory and time must be above zero");
        }
        match self.report.as_deref() {
            Some(path) if !container_path(path) => {
                Err("the report must be an absolute container file path")
            }
            _ => Ok(()),
        }
    }
}

impl Recipe {
    /// The command with every `{name}` replaced by its value, each value matched whole against
    /// its pattern; a value for an unknown name or a missing one is refused.
    pub fn fill(&self, values: &BTreeMap<String, String>) -> Result<Vec<String>, String> {
        if let Some(name) = values
            .keys()
            .find(|name| !self.parameters.contains_key(*name))
        {
            return Err(format!("`{name}` is not a parameter of this task"));
        }
        for (name, pattern) in &self.parameters {
            let value = values
                .get(name)
                .ok_or_else(|| format!("the parameter `{name}` has no value"))?;
            if !matcher(pattern).is_some_and(|matcher| matcher.is_match(value)) {
                return Err(format!("`{value}` does not fit the parameter `{name}`"));
            }
        }
        let command = self.command.as_deref().unwrap_or_default();
        Ok(command
            .iter()
            .map(|part| substitute(part, values))
            .collect())
    }
}

/// One argument with its `{name}` parts replaced in one pass, so a value is never read again.
fn substitute(part: &str, values: &BTreeMap<String, String>) -> String {
    let (mut filled, mut rest) = (String::new(), part);
    while let Some(start) = rest.find('{') {
        filled.push_str(&rest[..start]);
        let tail = &rest[start..];
        let end = tail.find('}').unwrap_or(tail.len() - 1);
        let value = values.get(&tail[1..end]);
        filled.push_str(value.map_or(&tail[..=end], String::as_str));
        rest = &tail[end + 1..];
    }
    filled.push_str(rest);
    filled
}

/// The names of the `{name}` parts of `command`, in order without repeats.
pub fn placeholders(command: &[String]) -> Result<BTreeSet<String>, &'static str> {
    let mut names = BTreeSet::new();
    for part in command {
        let mut rest = part.as_str();
        while let Some(start) = rest.find(['{', '}']) {
            let tail = &rest[start..];
            let end = tail.find('}').filter(|_| tail.starts_with('{'));
            let name = end.map(|end| &tail[1..end]);
            match name.filter(|name| crate::manifest::valid_name(name)) {
                Some(name) => names.insert(name.to_string()),
                None => return Err("a `{` or `}` in the command is not a parameter `{name}`"),
            };
            rest = &tail[end.unwrap_or_default() + 1..];
        }
    }
    Ok(names)
}

/// A whole-value matcher for `pattern`, bounded in size; `None` when it is not valid.
pub fn matcher(pattern: &str) -> Option<regex::Regex> {
    if pattern.len() > MAX_PATTERN {
        return None;
    }
    regex::RegexBuilder::new(&format!("^(?:{pattern})$"))
        .size_limit(1 << 20)
        .build()
        .ok()
}

/// `name@sha256:<64 lowercase hex>` without spaces.
fn pinned(image: &str) -> bool {
    let Some((name, digest)) = image.rsplit_once("@sha256:") else {
        return false;
    };
    image.len() <= MAX_IMAGE
        && !name.is_empty()
        && !image.chars().any(char::is_whitespace)
        && digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// An absolute path below `/` with no empty, `.` or `..` segment.
pub fn container_path(path: &str) -> bool {
    let Some(rest) = path.strip_prefix('/') else {
        return false;
    };
    path.len() <= MAX_PATH
        && !path.contains('\0')
        && rest
            .split('/')
            .all(|segment| !matches!(segment, "" | "." | ".."))
}
