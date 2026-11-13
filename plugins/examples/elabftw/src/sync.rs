//! The sync of one link: under a reservation, read both sides, compare, write each side and
//! keep the snapshot, creating the dataset of a new experiment first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::body::Body;
use crate::compare::{Decision, Side, Snapshot, decide, same};
use crate::elab::{self, MAX_CALL};
use crate::host::{Fault, Host, Kind, Result};
use crate::progress::{Link, Mark, Record, Reservation, Reserved, dataset_key, link_key};
use crate::values::{
    Experiment, Field, Settings, UPLOAD_PREFIX, Upload, Values, dataset_entity, dataset_patch,
    dataset_values, field_update, identified, identifier, link_fields, merge_file,
};

const RETRY_MS: u64 = 15 * 60 * 1000;
/// Rounds of read, compare and write per link while the dataset moves on in between.
const ROUNDS: usize = 3;
/// Room for the JSON around a body in one request.
const HEADROOM: usize = 4096;

/// What started a link's sync; it decides how much is read and checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The quick pass: file pages are read only when eLabFTW's uploads changed.
    Quick,
    Event,
    /// A failed link due again.
    Retry,
    /// The full reconciliation: also drift and storage sizes.
    Full,
    Manual,
}

/// One run's sync: its host, settings, run id and time.
pub struct Sync<'a> {
    pub host: &'a dyn Host,
    pub settings: &'a Settings,
    pub run: &'a str,
    pub now: u64,
    /// The running full pass, stamped into the links it checks.
    pass: Cell<u64>,
}

impl<'a> Sync<'a> {
    pub fn new(host: &'a dyn Host, settings: &'a Settings, run: &'a str, now: u64) -> Self {
        Self {
            host,
            settings,
            run,
            now,
            pass: Cell::new(0),
        }
    }

    /// Syncs one experiment's link, creating its dataset first when it has none, and returns
    /// the link's marks. Problems become marks; a link another run holds gives `None`.
    pub fn experiment(&self, id: u64, mode: Mode) -> Result<Option<BTreeSet<Mark>>> {
        let mut record = Record::load(self.host, &link_key(id))?;
        let link = &mut record.link;
        if link
            .reservation
            .as_ref()
            .is_some_and(|held| held.busy(self.run, self.now))
        {
            return Ok(None);
        }
        if mode == Mode::Retry && link.retry_at.is_some_and(|at| at > self.now) {
            return Ok(Some(link.marks.keys().copied().collect()));
        }
        // A change to eLabFTW that ended before its write-back: what was sent is the base, so
        // a later Aruna edit compares with it.
        let sent = link.reservation.take().and_then(|held| held.source);
        if let (Some((_, side)), Some(snapshot)) = (sent, link.snapshot.as_mut()) {
            snapshot.aruna = side;
        }
        let kind = if link.dataset.is_none() {
            Reserved::Dataset
        } else {
            Reserved::Change
        };
        link.experiment = Some(id);
        link.reservation = Some(Reservation::new(kind, self.run, self.now));
        match record.save(self.host) {
            Err(fault) if fault.kind == Kind::Conflict => return Ok(None),
            other => other?,
        }
        let outcome = self.link(&mut record, id, mode);
        let link = &mut record.link;
        link.reservation = None;
        match outcome {
            Ok(()) => {
                link.mark(Mark::Failed, None);
                link.mark(Mark::TooLarge, None);
                link.retry_at = None;
            }
            Err(fault) if fault.kind == Kind::Limit => {
                link.mark(Mark::TooLarge, Some(vec![fault.message]));
            }
            Err(fault) => link.fail(&fault.message, self.now, RETRY_MS),
        }
        if mode == Mode::Full {
            link.pass = self.pass.get();
        }
        let marks = link.marks.keys().copied().collect();
        record.save(self.host)?;
        Ok(Some(marks))
    }

    fn link(&self, record: &mut Record, id: u64, mode: Mode) -> Result<()> {
        for _ in 0..ROUNDS {
            match self.round(record, id, mode) {
                Err(fault) if fault.kind == Kind::Conflict => continue,
                other => return other,
            }
        }
        Err(Fault::new(
            Kind::Unavailable,
            "the dataset kept changing during the sync",
        ))
    }

    /// One read, compare and write of a link. A refused dataset revision ends the round with
    /// `Conflict`, after the snapshot took what eLabFTW already holds.
    fn round(&self, record: &mut Record, id: u64, mode: Mode) -> Result<()> {
        let Some(experiment) = elab::experiment(self.host, id)? else {
            record
                .link
                .mark(Mark::Gone, Some(vec!["experiment".into()]));
            return Ok(());
        };
        let tagged = experiment.tags.contains_key(&self.settings.tag);
        if experiment.state == 3 || !tagged {
            let what = if tagged { "experiment" } else { "tag" };
            record.link.mark(Mark::Gone, Some(vec![what.into()]));
            return Ok(());
        }
        record.link.mark(Mark::Gone, None);
        let blocked = record
            .link
            .marks
            .get(&Mark::Blocked)
            .and_then(|details| details.last());
        if blocked == Some(&experiment.modified) && mode != Mode::Manual {
            return Ok(());
        }
        record.link.mark(Mark::Blocked, None);
        let uploads = elab::uploads(self.host, id)?;
        let elab = experiment.values(Some(&uploads), self.settings);
        let writable = experiment.writable(self.settings.sync_user);
        record
            .link
            .mark(Mark::NoWrite, (!writable).then(|| vec!["canwrite".into()]));
        let dataset = match record.link.dataset.clone() {
            Some(dataset) => dataset,
            None => self.enrol(record, &experiment, &elab)?,
        };
        let summary = match self.host.summary(&dataset) {
            Err(fault) if fault.kind == Kind::NotFound => {
                record.link.mark(Mark::Gone, Some(vec!["dataset".into()]));
                return Ok(());
            }
            other => other?,
        };
        let known = record
            .link
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.elab.files.clone());
        let read_files = mode != Mode::Quick || known.is_none() || known != Side::of(&elab).files;
        let files = if read_files {
            Some(self.all_files(&dataset)?)
        } else {
            None
        };
        let aruna = dataset_values(&summary.json, files.as_deref(), &dataset, self.settings);
        let snapshot = (record.link.snapshot.clone()).unwrap_or_else(|| Snapshot::agreed(&aruna));
        let forced = record.link.force.clone();
        let forced = forced
            .as_ref()
            .map(|(origin, fields)| (*origin, fields.as_slice()));
        let decision = decide(&snapshot, &elab, &aruna, forced);
        let details = |fields: &[Field]| -> Option<Vec<String>> {
            let (left, right) = (Side::of(&elab), Side::of(&aruna));
            let detail = |field: &Field| {
                let print = |side: &Side| {
                    side.print(*field)
                        .map(|print| print.short())
                        .unwrap_or_default()
                };
                format!(
                    "{} elabftw {} aruna {}",
                    field.name(),
                    print(&left),
                    print(&right)
                )
            };
            (!fields.is_empty()).then(|| fields.iter().map(detail).collect())
        };
        record
            .link
            .mark(Mark::Conflict, details(&decision.conflicts));
        if mode == Mode::Full {
            record.link.mark(Mark::OutOfSync, details(&decision.drift));
            let changed = self.storage(&uploads)?;
            record.link.mark(
                Mark::ChangedInStorage,
                (!changed.is_empty()).then_some(changed),
            );
        }
        let fields_missing =
            experiment.missing_fields() || experiment.linked().as_deref() != Some(&dataset);
        let to_elab = !decision.to_elab.is_empty() || fields_missing;
        let after = match writable && to_elab {
            true => {
                let sent = (&summary.revision, &aruna);
                self.write_elab(
                    record,
                    &experiment,
                    &decision,
                    (&dataset, fields_missing),
                    sent,
                )?
            }
            false => experiment.clone(),
        };
        let elab_after = after.values(Some(&uploads), self.settings);
        let written = (writable && to_elab).then_some(&decision.to_elab);
        // eLabFTW's stored form of a body the plugin sent goes back, when eLabFTW kept a new
        // body at all; a lost write leaves Aruna's edit for the drift check instead.
        let sent_body = written.is_some_and(|written| written.contains(&Field::Body));
        let body_free =
            !decision.to_elab.contains(&Field::Body) && !decision.conflicts.contains(&Field::Body);
        let stored =
            (sent_body && after.body != experiment.body) || (snapshot.pending && body_free);
        let mut target = aruna.clone();
        let mut fields = Vec::new();
        for field in [Field::Title, Field::Tags, Field::Body, Field::Props] {
            let mut wanted = elab.clone();
            if field == Field::Body && stored {
                wanted = elab_after.clone();
            } else if !decision.to_aruna.contains(&field) {
                continue;
            }
            if let (Field::Tags, Some(merged)) = (field, &decision.tags) {
                wanted.tags = merged.clone();
            }
            let differs = match field {
                Field::Body => {
                    let plain = Some(wanted.body.plain());
                    wanted.body != target.body || target.description != plain
                }
                _ => !same(field, &wanted, &target),
            };
            if differs {
                copy(field, &wanted, &mut target);
                fields.push(field);
            }
        }
        let mut revision = summary.revision.clone();
        let mut patch = dataset_patch(&target, &fields);
        // A dataset made in Aruna gets the experiment's identifier, unless it holds another kind.
        let free = summary.json.get("identifier").is_none_or(Value::is_object);
        if identified(&summary.json).is_none() && free {
            patch["identifier"] = identifier(id, self.settings);
        }
        if patch.as_object().is_some_and(|patch| !patch.is_empty()) {
            let size = serde_json::to_vec(&patch).map_or(0, |bytes| bytes.len());
            if size + HEADROOM > MAX_CALL {
                return Err(Fault::new(
                    Kind::Limit,
                    format!("Aruna write of {size} bytes"),
                ));
            }
            match self.host.patch(&dataset, &patch, &revision) {
                Ok(next) => revision = next,
                Err(fault) if fault.kind == Kind::Conflict && written.is_some() => {
                    // eLabFTW holds what was sent; an Aruna edit made since compares with that.
                    // Fields still owed to Aruna keep their eLabFTW base, so they go next round.
                    let mut moved = snapshot.clone();
                    let owed = [&decision.conflicts[..], &decision.to_aruna[..]].concat();
                    moved.elab.update(&elab_after, &owed);
                    moved.aruna.update(&aruna, &decision.conflicts);
                    moved.pending |= stored;
                    moved.modified = after.modified.clone();
                    record.link.snapshot = Some(moved);
                    record.link.written = Some(after.modified.clone());
                    return Err(fault);
                }
                Err(fault) => return Err(fault),
            }
        }
        if decision.to_aruna.contains(&Field::Files)
            && let Some(files) = &files
        {
            revision = self.put_files(&dataset, &elab, files, revision)?;
            target.files = elab.files.clone();
        }
        let mut kept = decision.conflicts.clone();
        if !writable {
            kept.extend(&decision.to_elab);
        }
        let mut next = snapshot;
        next.elab.update(&elab_after, &kept);
        next.aruna.update(&target, &kept);
        next.modified = after.modified.clone();
        next.revision = revision;
        next.pending = false;
        let link = &mut record.link;
        link.snapshot = Some(next);
        link.force = None;
        if written.is_some() {
            link.written = Some(after.modified);
        }
        Ok(())
    }

    /// The dataset of an experiment without a link: the one its link field names if that
    /// still exists, else a keyed create that a retry after an unknown outcome repeats.
    fn enrol(&self, record: &mut Record, experiment: &Experiment, elab: &Values) -> Result<String> {
        let found = experiment
            .linked()
            .filter(|dataset| self.host.summary(dataset).is_ok());
        let dataset = match found {
            Some(dataset) => dataset,
            None => {
                let key = format!("elabftw/{}", experiment.id);
                let entity = dataset_entity(elab, experiment.id, self.settings);
                let size = serde_json::to_vec(&entity).map_or(0, |bytes| bytes.len());
                if size + HEADROOM > MAX_CALL {
                    return Err(Fault::new(
                        Kind::Limit,
                        format!("dataset create of {size} bytes"),
                    ));
                }
                self.host.create(&key, &entity, &key)?.resource
            }
        };
        record.link.dataset = Some(dataset.clone());
        record.link.snapshot = None;
        record.save(self.host)?;
        self.point(&dataset, experiment.id)?;
        Ok(dataset)
    }

    /// Makes `dataset/<id>` name the linked experiment.
    fn point(&self, dataset: &str, experiment: u64) -> Result<()> {
        let mut pointer = Record::load(self.host, &dataset_key(dataset))?;
        if pointer.link.experiment == Some(experiment) {
            return Ok(());
        }
        pointer.link = Link {
            experiment: Some(experiment),
            dataset: Some(dataset.to_string()),
            mark_page: pointer.link.mark_page,
            ..Link::default()
        };
        pointer.save(self.host)
    }

    fn all_files(&self, dataset: &str) -> Result<Vec<Value>> {
        let mut files = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.host.files(dataset, cursor.as_deref())?;
            files.extend(page.files);
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(files),
            }
        }
    }

    /// Upload ids whose object size differs from what eLabFTW recorded, or whose object is gone.
    fn storage(&self, uploads: &[Upload]) -> Result<Vec<String>> {
        let mut changed = Vec::new();
        for upload in uploads.iter().filter(|upload| upload.state != 3) {
            match self.host.object_size(&upload.path(self.settings)) {
                Ok(size) if size == upload.size => {}
                Ok(_) => changed.push(format!("upload {}", upload.id)),
                Err(fault) if fault.kind == Kind::NotFound => {
                    changed.push(format!("upload {} missing", upload.id));
                }
                Err(fault) => return Err(fault),
            }
        }
        Ok(changed)
    }

    /// Writes the fields that go to eLabFTW and the link fields, then reads the experiment
    /// back. The reservation records the Aruna revision and values it sends from first.
    fn write_elab(
        &self,
        record: &mut Record,
        experiment: &Experiment,
        decision: &Decision,
        (dataset, fields_missing): (&str, bool),
        (revision, aruna): (&String, &Values),
    ) -> Result<Experiment> {
        if let Some(held) = record.link.reservation.as_mut() {
            held.source = Some((revision.clone(), Side::of(aruna)));
        }
        record.save(self.host)?;
        let id = experiment.id;
        for field in &decision.to_elab {
            match field {
                Field::Title => elab::patch(self.host, id, json!({ "title": aruna.title }))?,
                Field::Tags => {
                    let wanted = decision.tags.as_ref().unwrap_or(&aruna.tags);
                    for tag in wanted
                        .iter()
                        .filter(|tag| !experiment.tags.contains_key(*tag))
                    {
                        elab::add_tag(self.host, id, tag)?;
                    }
                    let extra = (experiment.tags.iter())
                        .filter(|(tag, _)| !wanted.contains(*tag) && **tag != self.settings.tag);
                    for (_, tag) in extra {
                        elab::remove_tag(self.host, id, *tag)?;
                    }
                }
                Field::Body => {
                    let body = match decision.described {
                        true => Body::from_plain(
                            aruna.description.as_deref().unwrap_or_default(),
                            aruna.body.format,
                        ),
                        false => aruna.body.clone(),
                    };
                    let change =
                        json!({ "body": body.text, "content_type": body.format.content_type() });
                    elab::patch(self.host, id, change)?;
                }
                Field::Props | Field::Files | Field::Links => {}
            }
        }
        let links = aruna.links.clone().unwrap_or_else(|| {
            experiment
                .values(None, self.settings)
                .links
                .unwrap_or_default()
        });
        let mut blocked = None;
        if fields_missing {
            match experiment.clashing_field() {
                Some(name) => blocked = Some(name.to_string()),
                None => {
                    let merge =
                        json!({ "metadatamerge": link_fields(self.settings, dataset, &links) });
                    match elab::patch(self.host, id, merge) {
                        Err(fault) if fault.kind == Kind::Invalid => blocked = Some(fault.message),
                        other => other?,
                    }
                }
            }
        } else if decision.to_elab.contains(&Field::Links) {
            elab::patch(
                self.host,
                id,
                field_update(self.settings, Some(dataset), &links),
            )?;
        }
        let after = elab::experiment(self.host, id)?;
        let after = after.ok_or_else(|| Fault::new(Kind::NotFound, "the experiment is gone"))?;
        if let Some(reason) = blocked {
            record
                .link
                .mark(Mark::Blocked, Some(vec![reason, after.modified.clone()]));
        }
        Ok(after)
    }

    /// Brings the eLabFTW-origin file entities in step with the uploads: each changed one is
    /// merged into the stored entity, so properties outside the mapping stay.
    fn put_files(
        &self,
        dataset: &str,
        elab: &Values,
        files: &[Value],
        mut revision: String,
    ) -> Result<String> {
        let stored: BTreeMap<&str, &Value> = (files.iter())
            .filter_map(|file| Some((file.get("@id")?.as_str()?, file)))
            .collect();
        let current = dataset_values(&Value::Null, Some(files), dataset, self.settings).files;
        let current = current.unwrap_or_default();
        let wanted = elab.files.clone().unwrap_or_default();
        for (id, facts) in &wanted {
            if current.get(id) != Some(facts) {
                let file = merge_file(stored.get(id.as_str()).copied(), id, facts);
                revision = self.host.put_file(dataset, &file, &revision)?;
            }
        }
        for id in current
            .keys()
            .filter(|id| id.starts_with(UPLOAD_PREFIX) && !wanted.contains_key(*id))
        {
            revision = self.host.remove_file(dataset, id, &revision)?;
        }
        Ok(revision)
    }
}

/// Takes one field of `from` into `to`.
fn copy(field: Field, from: &Values, to: &mut Values) {
    match field {
        Field::Title => to.title = from.title.clone(),
        Field::Tags => to.tags = from.tags.clone(),
        Field::Body => {
            to.body = from.body.clone();
            to.description = Some(from.body.plain());
        }
        Field::Props => to.props = from.props.clone(),
        Field::Files => to.files = from.files.clone(),
        Field::Links => to.links = from.links.clone(),
    }
}
