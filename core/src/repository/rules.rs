//! Declarative mapping rules: which crate entities become which repository objects and fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::RepositoryError;
use super::fields::{crate_root, schema_value, values};
use crate::metadata::{
    ProfileValidationCompleteness, ProfileValidationFinding, ProfileValidationSeverity,
};
use crate::structs::execution::harvest::RepositoryConnectorKind;
use crate::structs::storage::replication::{
    ArunaArn, ArunaArnType, VersionedObjectArn, W3idIdentifier,
};

static INVENIO: LazyLock<Result<Rules, String>> =
    LazyLock::new(|| serde_json::from_str(include_str!("invenio.json")).map_err(|e| e.to_string()));

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

/// A crate entity and what it becomes; the root is `./`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Mapped {
    pub entity_id: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
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

/// What each crate entity becomes, and the rules the crate breaks. Each entity goes to the
/// first target that selects it; entities no target selects stay out of the mapping.
pub fn preview(rules: &Rules, document: &Value) -> (Vec<Mapped>, Vec<ProfileValidationFinding>) {
    let graph = values(&document["@graph"]);
    let root_id = crate_root(document).and_then(|root| root["@id"].as_str());
    let local = |id: &str| if Some(id) == root_id { "./" } else { id }.to_string();
    let mut chosen: Vec<(&Value, &Target)> = Vec::new();
    for candidate in graph {
        let id = candidate["@id"].as_str();
        if id.is_none_or(|id| id.ends_with("ro-crate-metadata.json")) {
            continue;
        }
        let root = id == root_id;
        if let Some(target) = rules
            .targets
            .iter()
            .find(|t| selects(&t.select, candidate, root))
        {
            chosen.push((candidate, target));
        }
    }
    let target_of = |id: &str| {
        chosen
            .iter()
            .find(|(entity, _)| entity["@id"] == id)
            .map(|(_, target)| target.name.as_str())
    };
    let mut mapped = Vec::new();
    let mut findings = Vec::new();
    for (candidate, target) in &chosen {
        let id = local(candidate["@id"].as_str().unwrap_or_default());
        let group = target.group.as_ref().and_then(|group| {
            let named = values(schema_value(candidate, &group.property))
                .iter()
                .filter_map(|value| value["@id"].as_str())
                .find(|named| target_of(named) == Some(group.each.as_str()))?;
            Some(local(named))
        });
        mapped.push(Mapped {
            entity_id: id.clone(),
            target: target.name.clone(),
            group,
            field: None,
        });
        for field in &target.fields {
            let Some(value) = field
                .property
                .iter()
                .map(|property| schema_value(candidate, property))
                .find(|value| !value.is_null())
            else {
                continue;
            };
            for item in values(value) {
                let entity_id = item["@id"].as_str().map_or_else(|| id.clone(), local);
                let entry = Mapped {
                    entity_id,
                    target: target.name.clone(),
                    group: None,
                    field: Some(field.field.clone()),
                };
                if !mapped.contains(&entry) {
                    mapped.push(entry);
                }
            }
        }
        for relation in &target.relations {
            let related = values(schema_value(candidate, &relation.property))
                .iter()
                .filter_map(|value| value["@id"].as_str())
                .filter(|named| target_of(named) == Some(relation.target.as_str()))
                .count();
            if related < relation.min {
                findings.push(finding(
                    "mapping_violation",
                    Some(id.clone()),
                    Some(relation.property.clone()),
                    format!("{}/relation", target.name),
                    format!(
                        "Each {} needs {} {} through {}.",
                        target.name, relation.min, relation.target, relation.property
                    ),
                ));
            }
        }
    }
    for target in &rules.targets {
        let count = chosen.iter().filter(|(_, t)| t.name == target.name).count();
        if count < target.min {
            findings.push(finding(
                "mapping_violation",
                Some("./".into()),
                None,
                format!("{}/min", target.name),
                format!("The crate needs at least {} {}.", target.min, target.name),
            ));
        }
        if let Some(max) = target.content.max_files.filter(|max| count > *max) {
            findings.push(finding(
                "content_violation",
                Some("./".into()),
                None,
                format!("{}/max_files", target.name),
                format!(
                    "The crate has {count} {}, more than the {max} allowed.",
                    target.name
                ),
            ));
        }
    }
    (mapped, findings)
}

/// A rule finding in the Profile finding format.
pub fn finding(
    code: &str,
    focus_node: Option<String>,
    path: Option<String>,
    rule: String,
    message: String,
) -> ProfileValidationFinding {
    ProfileValidationFinding {
        code: code.to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node,
        path,
        rule,
        message,
        profile_revision: None,
        completeness: ProfileValidationCompleteness::Complete,
    }
}

fn selects(select: &Select, candidate: &Value, root: bool) -> bool {
    if select.root {
        return root;
    }
    let id = candidate["@id"].as_str().unwrap_or_default();
    let types = values(&candidate["@type"])
        .iter()
        .filter_map(Value::as_str)
        .map(schema_name)
        .collect::<Vec<_>>();
    let formats = values(schema_value(candidate, "encodingFormat"))
        .iter()
        .filter_map(|format| format.as_str().or_else(|| format["@id"].as_str()))
        .collect::<Vec<_>>();
    let lower = id.to_ascii_lowercase();
    !root
        && (select.types.is_empty() || select.types.iter().any(|t| types.contains(&t.as_str())))
        && (select.formats.is_empty()
            || select.formats.iter().any(|f| formats.contains(&f.as_str())))
        && (select.extensions.is_empty()
            || select
                .extensions
                .iter()
                .any(|e| lower.ends_with(&e.to_ascii_lowercase())))
        && select
            .with_bytes
            .is_none_or(|wanted| has_bytes(candidate) == wanted)
}

fn schema_name(kind: &str) -> &str {
    ["schema:", "http://schema.org/", "https://schema.org/"]
        .into_iter()
        .find_map(|prefix| kind.strip_prefix(prefix))
        .unwrap_or(kind)
}

/// An entity carries Aruna bytes when it or its content URL names an Aruna object; any other
/// web address is a data entity on the web.
fn has_bytes(candidate: &Value) -> bool {
    let id = candidate["@id"].as_str().unwrap_or_default();
    let aruna = std::iter::once(id)
        .chain(
            values(schema_value(candidate, "contentUrl"))
                .iter()
                .filter_map(Value::as_str),
        )
        .any(|value| {
            W3idIdentifier::parse(value).is_ok()
                || VersionedObjectArn::parse(value).is_ok()
                || ArunaArn::parse(value)
                    .is_ok_and(|arn| arn.resource_type == ArunaArnType::ContentHash)
        });
    aruna || !(id.starts_with("https://") || id.starts_with("http://"))
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
