//! Snapshots of what each side stored after the last sync, as fingerprints, and the three-way
//! compare that decides per field what goes where and what is a conflict.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::body::Body;
use crate::values::{Field, Values};

/// A tag set is kept in the snapshot only below this many serialized bytes.
pub const MAX_TAG_SET: usize = 8 * 1024;

/// The byte length and SHA-256 of a value, as `<length>:<hex>`; never the value itself.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Print(String);

impl Print {
    pub fn of(bytes: &[u8]) -> Self {
        Self(format!(
            "{}:{}",
            bytes.len(),
            hex::encode(Sha256::digest(bytes))
        ))
    }

    fn json(value: &impl Serialize) -> Self {
        Self::of(&serde_json::to_vec(value).unwrap_or_default())
    }

    /// The length of the value it was made from.
    pub fn len(&self) -> u64 {
        let length = self.0.split(':').next().unwrap_or_default();
        length.parse().unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The length and the first 12 hex digits, for a mark's details.
    pub fn short(&self) -> String {
        let (length, hash) = self.0.split_once(':').unwrap_or_default();
        format!("{length}:{}", hash.get(..12).unwrap_or(hash))
    }

    /// The canonical form of a body, with its format.
    pub fn body(body: &Body) -> Self {
        Self::of(format!("{}\n{}", body.format.mime(), body.canonical()).as_bytes())
    }

    /// The stored text of a body, with its format.
    pub fn raw(body: &Body) -> Self {
        Self::of(format!("{}\n{}", body.format.mime(), body.text).as_bytes())
    }
}

/// The prints of what one side stored after the last sync.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Side {
    pub title: Print,
    pub tags: Print,
    /// The tag set itself while it is small, so tag changes on both sides can be merged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_set: Option<BTreeSet<String>>,
    /// The canonical body; `raw` is the stored text, so a cosmetic change is seen too.
    pub body: Print,
    pub raw: Print,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Print>,
    pub props: Print,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Print>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Print>,
}

impl Side {
    pub fn of(values: &Values) -> Self {
        let mut side = Self::default();
        side.update(values, &[]);
        side
    }

    /// Takes the prints of `values`, except for `kept` fields and parts this run did not read.
    pub fn update(&mut self, values: &Values, kept: &[Field]) {
        let take = |field| !kept.contains(&field);
        if take(Field::Title) {
            self.title = Print::of(values.title.as_bytes());
        }
        if take(Field::Tags) {
            self.tags = Print::json(&values.tags);
            let small = serde_json::to_vec(&values.tags).map_or(0, |bytes| bytes.len());
            self.tag_set = (small < MAX_TAG_SET).then(|| values.tags.clone());
        }
        if take(Field::Body) {
            self.body = Print::body(&values.body);
            self.raw = Print::raw(&values.body);
            self.description = values
                .description
                .as_deref()
                .map(|text| Print::of(text.as_bytes()));
        }
        if take(Field::Props) {
            self.props = Print::json(&values.props);
        }
        if let (true, Some(files)) = (take(Field::Files), &values.files) {
            self.files = Some(Print::json(files));
        }
        if let (true, Some(links)) = (take(Field::Links), &values.links) {
            self.links = Some(Print::json(links));
        }
    }

    /// Takes the prints of `fields` from `other`; every other field keeps its base.
    pub fn take(&mut self, other: &Side, fields: &[Field]) {
        for field in fields {
            match field {
                Field::Title => self.title = other.title.clone(),
                Field::Tags => {
                    self.tags = other.tags.clone();
                    self.tag_set = other.tag_set.clone();
                }
                Field::Body => {
                    self.body = other.body.clone();
                    self.raw = other.raw.clone();
                    self.description = other.description.clone();
                }
                Field::Props => self.props = other.props.clone(),
                Field::Files => self.files = other.files.clone(),
                Field::Links => self.links = other.links.clone(),
            }
        }
    }

    /// The print of one field; for the body, its canonical form.
    pub fn print(&self, field: Field) -> Option<&Print> {
        match field {
            Field::Title => Some(&self.title),
            Field::Tags => Some(&self.tags),
            Field::Body => Some(&self.body),
            Field::Props => Some(&self.props),
            Field::Files => self.files.as_ref(),
            Field::Links => self.links.as_ref(),
        }
    }

    /// Whether `values` differ from this side's prints in `field`; parts not read never do.
    fn changed(&self, field: Field, values: &Values) -> bool {
        match field {
            Field::Title => self.title != Print::of(values.title.as_bytes()),
            Field::Tags => self.tags != Print::json(&values.tags),
            Field::Body => self.body != Print::body(&values.body) || self.edited(values),
            Field::Props => self.props != Print::json(&values.props),
            Field::Files => values
                .files
                .as_ref()
                .is_some_and(|files| self.files != Some(Print::json(files))),
            Field::Links => values
                .links
                .as_ref()
                .is_some_and(|links| self.links != Some(Print::json(links))),
        }
    }

    /// Whether only the plain text description changed, which an Aruna user edits by hand.
    fn edited(&self, values: &Values) -> bool {
        let description = values
            .description
            .as_deref()
            .map(|text| Print::of(text.as_bytes()));
        description.is_some() && description != self.description
    }
}

/// What both sides stored after the last sync, and the versions the plugin's own writes made.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub elab: Side,
    pub aruna: Side,
    /// eLabFTW's `modified_at` after the plugin's last read.
    pub modified: String,
    /// The dataset revision after the plugin's last read or write.
    pub revision: String,
    /// eLabFTW holds a body the plugin sent whose stored form the dataset has not taken yet.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pending: bool,
}

impl Snapshot {
    /// The snapshot of a linked pair that agrees on `base`, as after a create or an adoption.
    pub fn agreed(base: &Values) -> Self {
        let mut elab = base.clone();
        elab.description = None;
        Self {
            elab: Side::of(&elab),
            aruna: Side::of(base),
            ..Self::default()
        }
    }
}

/// The side whose values win, for a settled conflict.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Elabftw,
    Aruna,
}

/// What one compare decided.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decision {
    pub to_aruna: Vec<Field>,
    pub to_elab: Vec<Field>,
    /// Changed on both sides to different values; neither side is written.
    pub conflicts: Vec<Field>,
    /// Different on the two sides while neither changed since the snapshot.
    pub drift: Vec<Field>,
    /// The merged tag set after tag changes on both sides.
    pub tags: Option<BTreeSet<String>>,
    /// Aruna changed only its description, so the eLabFTW body is built from it.
    pub described: bool,
}

/// Whether the two sides hold the same value of `field`; unread parts count as the same.
pub fn same(field: Field, elab: &Values, aruna: &Values) -> bool {
    match field {
        Field::Title => elab.title == aruna.title,
        Field::Tags => elab.tags == aruna.tags,
        Field::Body => {
            let format = elab.body.format == aruna.body.format;
            format && elab.body.canonical() == aruna.body.canonical()
        }
        Field::Props => elab.props == aruna.props,
        Field::Files => elab.files.is_none() || aruna.files.is_none() || elab.files == aruna.files,
        Field::Links => elab.links.is_none() || aruna.links.is_none() || elab.links == aruna.links,
    }
}

/// The three-way compare of both sides against the snapshot. `forced` fields take the
/// values of the named side, as a settled conflict asks.
pub fn decide(
    snapshot: &Snapshot,
    elab: &Values,
    aruna: &Values,
    forced: Option<(Origin, &[Field])>,
) -> Decision {
    let mut decision = Decision::default();
    for field in Field::ALL {
        let differ = !same(field, elab, aruna);
        let force = forced.and_then(|(origin, fields)| fields.contains(&field).then_some(origin));
        match (field, force) {
            (Field::Props | Field::Files, _) if differ => decision.to_aruna.push(field),
            (Field::Links, _) if differ => decision.to_elab.push(field),
            (Field::Props | Field::Files | Field::Links, _) => {}
            (_, Some(Origin::Elabftw)) if differ => decision.to_aruna.push(field),
            (_, Some(Origin::Aruna)) if differ => decision.to_elab.push(field),
            (_, Some(_)) => {}
            (_, None) => two_way(field, snapshot, (elab, aruna), &mut decision),
        }
    }
    decision
}

fn two_way(
    field: Field,
    snapshot: &Snapshot,
    (elab, aruna): (&Values, &Values),
    decision: &mut Decision,
) {
    let from_elab = snapshot.elab.changed(field, elab);
    let from_aruna = snapshot.aruna.changed(field, aruna);
    let differ = !same(field, elab, aruna);
    match (from_elab, from_aruna) {
        (false, false) if differ => decision.drift.push(field),
        (true, false) if differ => decision.to_aruna.push(field),
        (false, true) => {
            let text = snapshot.aruna.body != Print::body(&aruna.body);
            if field == Field::Body && !text {
                // The text is as synced, so the plain text description is the edit.
                decision.described = true;
                decision.to_elab.push(field);
            } else if differ {
                decision.to_elab.push(field);
            }
        }
        (true, true) if differ => match merged_tags(field, snapshot, elab, aruna) {
            Some(tags) => {
                decision.tags = Some(tags);
                decision.to_aruna.push(field);
                decision.to_elab.push(field);
            }
            None => decision.conflicts.push(field),
        },
        _ => {}
    }
}

/// Tag changes on both sides merge against the base set, when both snapshots still hold it.
fn merged_tags(
    field: Field,
    snapshot: &Snapshot,
    elab: &Values,
    aruna: &Values,
) -> Option<BTreeSet<String>> {
    if field != Field::Tags {
        return None;
    }
    let base = snapshot.elab.tag_set.as_ref()?;
    if snapshot.aruna.tag_set.as_ref() != Some(base) {
        return None;
    }
    let removed = |side: &BTreeSet<String>| base.difference(side).cloned().collect::<BTreeSet<_>>();
    let (gone_elab, gone_aruna) = (removed(&elab.tags), removed(&aruna.tags));
    let union = elab.tags.union(&aruna.tags).cloned();
    Some(
        union
            .filter(|tag| !gone_elab.contains(tag) && !gone_aruna.contains(tag))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::Format;

    fn values(title: &str, tags: &[&str], body: &str) -> Values {
        Values {
            title: title.into(),
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            body: Body::new(body, Format::Html),
            ..Values::default()
        }
    }

    fn aruna(title: &str, tags: &[&str], body: &str) -> Values {
        let mut values = values(title, tags, body);
        values.description = Some(values.body.plain());
        values
    }

    fn base() -> Snapshot {
        Snapshot::agreed(&aruna("T", &["a"], "<p>x</p>"))
    }

    #[test]
    fn one_sided_changes() {
        let decision = decide(
            &base(),
            &values("T2", &["a"], "<p>x</p>"),
            &aruna("T", &["a"], "<p>x</p>"),
            None,
        );
        assert_eq!(
            (decision.to_aruna, decision.to_elab),
            (vec![Field::Title], vec![])
        );
        let decision = decide(
            &base(),
            &values("T", &["a"], "<p>x</p>"),
            &aruna("T", &["a"], "<p>y</p>"),
            None,
        );
        assert_eq!(
            (decision.to_elab, decision.described),
            (vec![Field::Body], false)
        );
        let same_change = decide(
            &base(),
            &values("U", &["a"], "<p>x</p>"),
            &aruna("U", &["a"], "<p>x</p>"),
            None,
        );
        assert_eq!(same_change, Decision::default());
    }

    #[test]
    fn conflicts_and_merges() {
        let decision = decide(
            &base(),
            &values("E", &["a", "e"], "<p>x</p>"),
            &aruna("A", &["b"], "<p>x</p>"),
            None,
        );
        assert_eq!(decision.conflicts, vec![Field::Title]);
        assert_eq!(decision.tags, Some(["b", "e"].map(String::from).into()));
        assert_eq!(
            (decision.to_aruna, decision.to_elab),
            (vec![Field::Tags], vec![Field::Tags])
        );
        let forced = decide(
            &base(),
            &values("E", &["a"], "<p>x</p>"),
            &aruna("A", &["a"], "<p>x</p>"),
            Some((Origin::Aruna, &[Field::Title])),
        );
        assert_eq!(
            (forced.to_elab, forced.conflicts),
            (vec![Field::Title], vec![])
        );
        // Without the base tag set, tag changes on both sides are a conflict.
        let mut large = base();
        large.elab.tag_set = None;
        let decision = decide(
            &large,
            &values("T", &["e"], "<p>x</p>"),
            &aruna("T", &["b"], "<p>x</p>"),
            None,
        );
        assert_eq!(decision.conflicts, vec![Field::Tags]);
    }

    #[test]
    fn drift_and_descriptions() {
        let mut lost = base();
        lost.aruna = Side::of(&aruna("A", &["a"], "<p>x</p>"));
        let decision = decide(
            &lost,
            &values("T", &["a"], "<p>x</p>"),
            &aruna("A", &["a"], "<p>x</p>"),
            None,
        );
        assert_eq!(decision.drift, vec![Field::Title]);
        let mut edited = aruna("T", &["a"], "<p>x</p>");
        edited.description = Some("new words".into());
        let decision = decide(&base(), &values("T", &["a"], "<p>x</p>"), &edited, None);
        assert_eq!(
            (decision.to_elab, decision.described),
            (vec![Field::Body], true)
        );
    }

    #[test]
    fn one_way_fields() {
        let mut elab = values("T", &["a"], "<p>x</p>");
        elab.props.status = "Done".into();
        elab.links = Some(BTreeSet::new());
        let mut other = aruna("T", &["a"], "<p>x</p>");
        other.links = Some(["https://p/f".to_string()].into());
        other.props.status = "Edited in Aruna".into();
        let decision = decide(&base(), &elab, &other, None);
        assert_eq!(
            (decision.to_aruna, decision.to_elab),
            (vec![Field::Props], vec![Field::Links])
        );
        assert!(Print::of(b"abc").short().starts_with("3:ba7816bf8f01"));
    }
}
