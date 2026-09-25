//! Declarative mapping rules: which crate entities become which repository objects and fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;

use serde::Deserialize;

use super::RepositoryError;
use crate::structs::execution::harvest::RepositoryConnectorKind;

static INVENIO: LazyLock<Result<Rules, String>> =
    LazyLock::new(|| toml::from_str(include_str!("invenio.toml")).map_err(|e| e.to_string()));

/// The rules of one repository kind, in the order their targets select entities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub targets: Vec<Target>,
}

/// One kind of repository object, such as a record, a file or a sample.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub name: String,
    pub select: Select,
    #[serde(default)]
    pub group: Option<Group>,
    #[serde(default)]
    pub relations: Vec<Relation>,
    /// The crate needs at least this many entities of the target.
    #[serde(default)]
    pub min: usize,
    #[serde(default)]
    pub content: Content,
    #[serde(default)]
    pub fields: Vec<Field>,
}

/// Which entities a target takes; every given condition must hold.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Select {
    #[serde(default)]
    pub root: bool,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub formats: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
    /// Whether the entity must carry Aruna bytes, or must not.
    #[serde(default)]
    pub with_bytes: Option<bool>,
}

/// Groups a target's entities by the entity of target `each` they name through `property`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub each: String,
    pub property: String,
    /// Name markers of two files that form one pair, such as `_1` and `_2`.
    #[serde(default)]
    pub pair: Option<[String; 2]>,
}

/// Each entity must name at least `min` entities of `target` through `property`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    pub property: String,
    pub target: String,
    #[serde(default = "one")]
    pub min: usize,
}

/// Checks of the file bytes, run by the export before any remote write.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Content {
    pub max_files: Option<usize>,
    pub max_file_bytes: Option<u64>,
    pub max_total_bytes: Option<u64>,
    /// A file format the bytes must have, such as `fastq` or `bam`.
    pub format: Option<String>,
    /// Whether each file needs an MD5 checksum.
    #[serde(default)]
    pub md5: bool,
}

/// A repository field filled from the first of `property` the entity has.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub property: Vec<String>,
    pub field: String,
    pub convert: Convert,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Convert {
    Text,
    Date,
    Persons,
    Licenses,
    Keywords,
    Identifiers,
    Publisher,
}

fn one() -> usize {
    1
}

/// The embedded rules of a kind; `None` for kinds without rules.
pub fn rules(kind: RepositoryConnectorKind) -> Result<Option<&'static Rules>, RepositoryError> {
    match kind {
        RepositoryConnectorKind::Invenio => INVENIO
            .as_ref()
            .map(Some)
            .map_err(|_| RepositoryError("invalid embedded mapping rules")),
        RepositoryConnectorKind::OaiPmh => Ok(None),
    }
}

impl Rules {
    pub fn target(&self, name: &str) -> Option<&Target> {
        self.targets.iter().find(|target| target.name == name)
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
