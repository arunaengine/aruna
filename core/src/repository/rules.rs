//! Declarative mapping rules: which crate entities become which repository objects and fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;

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

/// Archive files an export writes besides the data files: the crate metadata and the report.
pub const CRATE_FILES: [&str; 2] = ["ro-crate-metadata.json", "aruna-export-report.json"];

/// How many leading bytes of a file a format check reads.
pub const FORMAT_PREFIX: u64 = 64 * 1024;

/// The rules of one repository kind, in the order their targets select entities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub targets: Vec<Target>,
}

/// One kind of repository object, such as a record, a file or a sample.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub name: String,
    pub select: Select,
    /// The target also takes the `CRATE_FILES`; limits count the report even when an export
    /// writes none, so the preview and the export count the same files.
    #[serde(default)]
    pub crate_files: bool,
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
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
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

/// Groups a target's entities by the entity of target `each` they name through `property`;
/// every entity needs one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub each: String,
    pub property: String,
    /// Name markers of two files that form one pair in a group, such as `_1` and `_2`.
    #[serde(default)]
    pub pair: Option<[String; 2]>,
}

/// Each entity must name between `min` and `max` entities of `target` through `property`, or
/// with `inverse` be named by that many through it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Relation {
    pub property: String,
    pub target: String,
    #[serde(default = "one")]
    pub min: usize,
    #[serde(default)]
    pub max: Option<usize>,
    #[serde(default)]
    pub inverse: bool,
}

/// Checks of the target's files, run by the export before any remote write.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Content {
    pub max_files: Option<usize>,
    pub max_file_bytes: Option<u64>,
    pub max_total_bytes: Option<u64>,
    /// A file format the bytes must have, one of `fastq`, `bam` or `cram`.
    pub format: Option<String>,
}

/// A repository field filled from the values of `property`; with several present properties
/// the field takes their deduplicated union.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub property: Vec<String>,
    pub field: String,
    pub convert: Convert,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct Mapped {
    pub entity_id: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// One file of a target: its archive path, and its size and first bytes when known.
#[derive(Clone, Copy, Debug)]
pub struct FileFacts<'a> {
    pub path: &'a str,
    pub size: Option<u64>,
    pub prefix: Option<&'a [u8]>,
}

/// A format as the bytes it starts with and whether gzip or BGZF wraps them.
struct Format {
    name: &'static str,
    magic: &'static [u8],
    gzip: Gzip,
}

#[derive(PartialEq)]
enum Gzip {
    Never,
    Optional,
    Required,
}

const FORMATS: [Format; 3] = [
    Format {
        name: "fastq",
        magic: b"@",
        gzip: Gzip::Optional,
    },
    Format {
        name: "bam",
        magic: b"BAM\x01",
        gzip: Gzip::Required,
    },
    Format {
        name: "cram",
        magic: b"CRAM",
        gzip: Gzip::Never,
    },
];

fn one() -> usize {
    1
}

/// The embedded rules of a kind; `None` for kinds without rules.
pub fn rules(kind: RepositoryConnectorKind) -> Result<Option<&'static Rules>, RepositoryError> {
    super::descriptor(kind).map(|kind| kind.rules()).transpose()
}

/// Parses a kind's rules JSON and refuses rules the preview or export would not evaluate.
pub(super) fn load(json: &str) -> Result<Rules, String> {
    let rules: Rules = serde_json::from_str(json).map_err(|error| error.to_string())?;
    rules.validate()?;
    Ok(rules)
}

impl Rules {
    pub fn target(&self, name: &str) -> Option<&Target> {
        self.targets.iter().find(|target| target.name == name)
    }

    fn validate(&self) -> Result<(), String> {
        let mut names = HashSet::new();
        for target in &self.targets {
            if !names.insert(target.name.as_str()) {
                return Err(format!("target {} repeats", target.name));
            }
            let named = target
                .group
                .iter()
                .map(|group| &group.each)
                .chain(target.relations.iter().map(|relation| &relation.target));
            for name in named {
                if self.target(name).is_none() {
                    return Err(format!("{} names the unknown target {name}", target.name));
                }
            }
            if let Some(format) = &target.content.format
                && !FORMATS.iter().any(|known| known.name == format)
            {
                return Err(format!(
                    "{} requires the unknown format {format}",
                    target.name
                ));
            }
            if target
                .relations
                .iter()
                .any(|relation| relation.max.is_some_and(|max| max < relation.min))
            {
                return Err(format!("{} has a relation with max below min", target.name));
            }
        }
        Ok(())
    }
}

/// The value of `field` on an entity: the one present property, else the deduplicated union
/// of every present property.
pub fn field_value(entity: &Value, field: &Field) -> Value {
    let present = field
        .property
        .iter()
        .map(|property| schema_value(entity, property))
        .filter(|value| !value.is_null())
        .collect::<Vec<_>>();
    match present.as_slice() {
        [] => Value::Null,
        [value] => (*value).clone(),
        several => {
            let mut union = Vec::new();
            for value in several.iter().flat_map(|value| values(value)) {
                if !union.contains(value) {
                    union.push(value.clone());
                }
            }
            Value::Array(union)
        }
    }
}

/// What each crate entity becomes, and the rules the crate breaks. Each entity goes to the
/// first target that selects it; entities no target selects stay out of the mapping.
pub fn preview(rules: &Rules, document: &Value) -> (Vec<Mapped>, Vec<ProfileValidationFinding>) {
    let graph = values(&document["@graph"]);
    let root_id = crate_root(document).and_then(|root| root["@id"].as_str());
    let local = |id: &str| if Some(id) == root_id { "./" } else { id }.to_string();
    let mut chosen: Vec<(&Value, &str, &Target)> = Vec::new();
    for candidate in graph {
        let Some(id) = candidate["@id"].as_str() else {
            continue;
        };
        if id.ends_with("ro-crate-metadata.json") {
            continue;
        }
        let root = Some(id) == root_id;
        if let Some(target) = rules
            .targets
            .iter()
            .find(|t| selects(&t.select, candidate, root))
        {
            chosen.push((candidate, id, target));
        }
    }
    let target_of = chosen
        .iter()
        .map(|(_, id, target)| (*id, target.name.as_str()))
        .collect::<HashMap<_, _>>();
    let named = |entity: &Value, property: &str, target: &str| {
        values(schema_value(entity, property))
            .iter()
            .filter_map(|value| value["@id"].as_str())
            .filter(|named| target_of.get(named) == Some(&target))
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let mut mapped = Vec::new();
    let mut seen = HashSet::new();
    let mut findings = Vec::new();
    let mut groups: BTreeMap<(&str, String), Vec<String>> = BTreeMap::new();
    for (candidate, raw_id, target) in &chosen {
        let id = local(raw_id);
        let group = target.group.as_ref().and_then(|group| {
            let each = named(candidate, &group.property, &group.each);
            if each.is_empty() {
                findings.push(finding(
                    "mapping_violation",
                    Some(id.clone()),
                    Some(group.property.clone()),
                    format!("{}/group", target.name),
                    format!(
                        "Each {} needs a {} through {}.",
                        target.name, group.each, group.property
                    ),
                ));
            }
            each.first().map(|named| local(named))
        });
        if let Some(group) = &group {
            groups
                .entry((target.name.as_str(), group.clone()))
                .or_default()
                .push(id.clone());
        }
        let entry = Mapped {
            entity_id: id.clone(),
            target: target.name.clone(),
            group,
            field: None,
        };
        if seen.insert(entry.clone()) {
            mapped.push(entry);
        }
        for field in &target.fields {
            let value = field_value(candidate, field);
            for item in values(&value) {
                let entity_id = item["@id"].as_str().map_or_else(|| id.clone(), local);
                let entry = Mapped {
                    entity_id,
                    target: target.name.clone(),
                    group: None,
                    field: Some(field.field.clone()),
                };
                if seen.insert(entry.clone()) {
                    mapped.push(entry);
                }
            }
        }
    }
    for target in &rules.targets {
        findings.extend(relation_findings(target, &chosen, &local, &named));
        if let Some([first, second]) = target.group.as_ref().and_then(|group| group.pair.as_ref()) {
            for ((name, _), members) in &groups {
                if *name == target.name {
                    findings.extend(pair_findings(target, members, first, second));
                }
            }
        }
        if target.crate_files {
            mapped.extend(CRATE_FILES.iter().map(|path| Mapped {
                entity_id: (*path).to_string(),
                target: target.name.clone(),
                group: None,
                field: None,
            }));
        }
        let count = chosen
            .iter()
            .filter(|(_, _, t)| t.name == target.name)
            .count();
        if count < target.min {
            findings.push(finding(
                "mapping_violation",
                Some("./".into()),
                None,
                format!("{}/min", target.name),
                format!("The crate needs at least {} {}.", target.min, target.name),
            ));
        }
        let files = target_files(&target.name, &mapped, |id| Some(id.to_string()));
        let facts = files
            .iter()
            .map(|path| FileFacts {
                path,
                size: None,
                prefix: None,
            })
            .collect::<Vec<_>>();
        findings.extend(content_findings(target, &facts));
    }
    (mapped, findings)
}

/// Relation counts of every entity of `target`, named or, with `inverse`, naming.
fn relation_findings(
    target: &Target,
    chosen: &[(&Value, &str, &Target)],
    local: &impl Fn(&str) -> String,
    named: &impl Fn(&Value, &str, &str) -> Vec<String>,
) -> Vec<ProfileValidationFinding> {
    let mut findings = Vec::new();
    for relation in &target.relations {
        let mut inverse: HashMap<String, usize> = HashMap::new();
        if relation.inverse {
            for (entity, _, _) in chosen.iter().filter(|(_, _, t)| t.name == relation.target) {
                for id in named(entity, &relation.property, &target.name) {
                    *inverse.entry(id).or_default() += 1;
                }
            }
        }
        for (entity, id, _) in chosen.iter().filter(|(_, _, t)| t.name == target.name) {
            let count = if relation.inverse {
                inverse.get(*id).copied().unwrap_or_default()
            } else {
                named(entity, &relation.property, &relation.target).len()
            };
            if count >= relation.min && relation.max.is_none_or(|max| count <= max) {
                continue;
            }
            let amount = match relation.max {
                Some(max) if max == relation.min => format!("exactly {max}"),
                Some(max) => format!("{} to {max}", relation.min),
                None => format!("at least {}", relation.min),
            };
            let link = if relation.inverse {
                "naming it through"
            } else {
                "through"
            };
            findings.push(finding(
                "mapping_violation",
                Some(local(id)),
                Some(relation.property.clone()),
                format!("{}/relation", target.name),
                format!(
                    "Each {} needs {amount} {} {link} {}.",
                    target.name, relation.target, relation.property
                ),
            ));
        }
    }
    findings
}

/// Files of one group that carry a pair marker without the partner file.
fn pair_findings(
    target: &Target,
    members: &[String],
    first: &str,
    second: &str,
) -> Vec<ProfileValidationFinding> {
    let partner = |id: &str| {
        let (marker, other) = if id.contains(first) {
            (first, second)
        } else if id.contains(second) {
            (second, first)
        } else {
            return None;
        };
        let at = id.rfind(marker)?;
        Some(format!("{}{other}{}", &id[..at], &id[at + marker.len()..]))
    };
    members
        .iter()
        .filter(|id| partner(id).is_some_and(|partner| !members.contains(&partner)))
        .map(|id| {
            finding(
                "mapping_violation",
                Some(id.clone()),
                None,
                format!("{}/pair", target.name),
                format!("{id} needs its paired file ({first} and {second}) in the same group."),
            )
        })
        .collect()
}

/// The archive paths an export uploads for `target`: its mapped entities that `path` places in
/// the archive, and the `CRATE_FILES` the mapping lists for it.
pub fn target_files(
    target: &str,
    mapping: &[Mapped],
    path: impl Fn(&str) -> Option<String>,
) -> Vec<String> {
    mapping
        .iter()
        .filter(|mapped| mapped.target == target && mapped.field.is_none())
        .filter_map(|mapped| {
            let id = mapped.entity_id.as_str();
            if CRATE_FILES.contains(&id) {
                Some(id.to_string())
            } else {
                path(id)
            }
        })
        .collect()
}

/// The content rules of `target` checked against its files; unknown sizes and prefixes skip
/// the checks that need them.
pub fn content_findings(target: &Target, files: &[FileFacts<'_>]) -> Vec<ProfileValidationFinding> {
    let content = &target.content;
    let violation = |focus: &str, rule: &str, message: String| {
        finding(
            "content_violation",
            Some(focus.to_string()),
            None,
            format!("{}/{rule}", target.name),
            message,
        )
    };
    let mut findings = Vec::new();
    if let Some(max) = content.max_files.filter(|max| files.len() > *max) {
        let message = format!(
            "The export uploads {} files, more than the {max} allowed for {}.",
            files.len(),
            target.name
        );
        findings.push(violation("./", "max_files", message));
    }
    for file in files {
        if let (Some(max), Some(size)) = (content.max_file_bytes, file.size)
            && size > max
        {
            let message = format!("{} has {size} bytes, more than {max}.", file.path);
            findings.push(violation(file.path, "max_file_bytes", message));
        }
        if let (Some(format), Some(prefix)) = (&content.format, file.prefix)
            && !has_format(format, prefix)
        {
            let message = format!("{} is not a {format} file.", file.path);
            findings.push(violation(file.path, "format", message));
        }
    }
    let total = files.iter().filter_map(|file| file.size).sum::<u64>();
    if let Some(max) = content.max_total_bytes.filter(|max| total > *max) {
        let message = format!(
            "The {} files have {total} bytes, more than {max}.",
            target.name
        );
        findings.push(violation("./", "max_total_bytes", message));
    }
    findings
}

/// Whether a file's first bytes have `format`; gzip and BGZF members are decompressed first.
pub fn has_format(format: &str, prefix: &[u8]) -> bool {
    let Some(format) = FORMATS.iter().find(|known| known.name == format) else {
        return false;
    };
    if !prefix.starts_with(&[0x1f, 0x8b]) {
        return format.gzip != Gzip::Required && prefix.starts_with(format.magic);
    }
    if format.gzip == Gzip::Never {
        return false;
    }
    let mut decoder = flate2::read::MultiGzDecoder::new(prefix);
    let mut start = vec![0; format.magic.len()];
    let mut filled = 0;
    while filled < start.len() {
        match decoder.read(&mut start[filled..]) {
            Ok(0) | Err(_) => break,
            Ok(read) => filled += read,
        }
    }
    start[..filled] == *format.magic
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
