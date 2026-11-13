//! The sync of one install: links one at a time under a reservation, the timer's quick pass and
//! full reconciliation, dataset events, and the manual run's report and resolutions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::body::Body;
use crate::compare::{Decision, Origin, Side, Snapshot, decide, same};
use crate::elab::{self, MAX_CALL, Order};
use crate::host::{Fault, Host, Kind, Meter, Result};
use crate::progress::{
    Cursor, LINK_PAGE, Link, Listed, Mark, Pass, Phase, Record, Reservation, Reserved, Sent,
    dataset_key, failed_page, link_key, link_page, load, report, store,
};
use crate::values::{
    BASE_TEAM, BASE_USER, Experiment, Field, ID_FIELD, Settings, UPLOAD_PREFIX, Upload, Values,
    dataset_entity, dataset_patch, dataset_values, field_update, identified, identifier,
    link_fields, merge_file,
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

/// Host calls and bytes, as a run spends them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Room {
    pub calls: u32,
    pub bytes: u64,
}

/// What one run may spend, below the runtime's 1000 calls and 16 MiB, and the room a link and
/// one step inside it need: a link starts, and a step goes on, only while that room is left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub total: Room,
    pub link: Room,
    pub step: Room,
}

impl Default for Budget {
    fn default() -> Self {
        let room = |calls, bytes| Room { calls, bytes };
        Self {
            total: room(900, 15 << 20),
            link: room(60, 5 << 20),
            step: room(30, 2 << 20),
        }
    }
}

/// How one link's sync ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Synced(BTreeSet<Mark>),
    /// Another run holds the link, or a failed link is not due yet.
    Skipped,
    /// The run's budget ran out inside the link; a later run goes on from there.
    Spent,
}

/// One run's sync: its metered host, budget, settings, run id and time.
pub struct Sync<'a> {
    pub host: &'a Meter<'a>,
    budget: Budget,
    pub settings: &'a Settings,
    pub run: &'a str,
    pub now: u64,
    /// The running full pass, stamped into the links it checks.
    pass: Cell<u64>,
}

impl<'a> Sync<'a> {
    pub fn new(host: &'a Meter<'a>, settings: &'a Settings, run: &'a str, now: u64) -> Self {
        Self {
            host,
            budget: Budget::default(),
            settings,
            run,
            now,
            pass: Cell::new(0),
        }
    }

    pub fn with_budget(self, budget: Budget) -> Self {
        Self { budget, ..self }
    }

    /// Calls made and bytes moved by this run so far.
    pub fn used(&self) -> Room {
        let (calls, bytes) = self.host.used();
        Room { calls, bytes }
    }

    /// Whether `keep` is still left of the run's budget.
    fn room(&self, keep: Room) -> bool {
        let (used, total) = (self.used(), self.budget.total);
        used.calls.saturating_add(keep.calls) <= total.calls
            && used.bytes.saturating_add(keep.bytes) <= total.bytes
    }

    /// Room for one more step inside a link, or `Spent`.
    fn step(&self) -> Result<()> {
        match self.room(self.budget.step) {
            true => Ok(()),
            false => Err(Fault::new(Kind::Spent, "the run's budget is used")),
        }
    }

    /// The timer run: the bucket check, due failed links, the quick pass, then a chunk of the
    /// full reconciliation.
    pub fn poll(&self) -> Result<Value> {
        self.check_bucket()?;
        let (version, mut cursor) = match load::<Cursor>(self.host, "cursor")? {
            Some((version, cursor)) => (Some(version), cursor),
            None => (None, Cursor::default()),
        };
        let mut done = 0;
        // Failed links come first, from where the last run left this mark page; links not due
        // yet, or held by another run, cost one state read and no work.
        let (keys, more) = failed_page(self.host, cursor.retry_page)?;
        let after = cursor.retry_after.clone();
        let mut rest = (keys.into_iter())
            .filter(|key| after.as_ref().is_none_or(|after| key > after))
            .peekable();
        let mut stopped = false;
        while let Some(key) = rest.peek().cloned() {
            if !self.room(self.budget.link) {
                stopped = true;
                break;
            }
            match self.retry(&key) {
                Ok(Outcome::Synced(_)) => done += 1,
                Ok(Outcome::Skipped) => {}
                Ok(Outcome::Spent) => {
                    stopped = true;
                    break;
                }
                Err(fault) => self.guard(Err(fault)),
            }
            cursor.retry_after = Some(key);
            rest.next();
        }
        if !stopped {
            cursor.retry_page = if more { cursor.retry_page + 1 } else { 0 };
            cursor.retry_after = None;
        }
        let tag = &self.settings.tag;
        let entries = cursor.walk(|offset| {
            self.step()?;
            elab::list(self.host, tag, Order::Changed, offset)
        })?;
        for entry in entries {
            if !self.room(self.budget.link) {
                break;
            }
            if !self.own_write(&entry)? {
                match self.experiment(entry.id, Mode::Quick) {
                    // Another run holds it, or the budget ran out: the next run goes on here.
                    Ok(Outcome::Skipped | Outcome::Spent) => break,
                    Ok(Outcome::Synced(_)) => done += 1,
                    Err(fault) => self.guard(Err(fault)),
                }
            }
            cursor.advance(&entry.modified, entry.id);
        }
        // Another run that moved the cursor meanwhile wins; this run's entries come again.
        match store(self.host, "cursor", &cursor, version) {
            Err(fault) if fault.kind == Kind::Conflict => {}
            other => drop(other?),
        }
        self.full(false)?;
        Ok(json!({ "synced": done }))
    }

    /// A dataset event: the linked experiment's sync, or an experiment for a new dataset below
    /// `meta/elabftw/`. An event the plugin's own write caused is skipped.
    pub fn changed(&self, dataset: &str, path: &str, revision: &str) -> Result<()> {
        let pointer = load::<Link>(self.host, &dataset_key(dataset))?.map(|(_, link)| link);
        if let Some(experiment) = pointer.and_then(|link| link.experiment) {
            let link = load::<Link>(self.host, &link_key(experiment))?.map(|(_, link)| link);
            let seen = link
                .and_then(|link| link.snapshot)
                .map(|snapshot| snapshot.revision);
            if seen.is_some_and(|seen| revision <= seen.as_str()) {
                return Ok(());
            }
            return self.experiment(experiment, Mode::Event).map(drop);
        }
        let below = path
            .split_once("/meta/")
            .map(|(_, rest)| rest.starts_with("elabftw/"));
        if below != Some(true) {
            return Ok(());
        }
        self.dataset(dataset).map(drop)
    }

    /// A manual run: a report page, a settled conflict or create, a full pass, or one link.
    pub fn manual(&self, input: &Value) -> Result<Value> {
        if input.get("report").and_then(Value::as_bool) == Some(true) {
            let after = input
                .get("after")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            return report(self.host, after);
        }
        if let Some(experiment) = input.get("resolve").and_then(Value::as_u64) {
            let keep: Origin = serde_json::from_value(input["keep"].clone())
                .map_err(|_| Fault::new(Kind::Invalid, "keep is \"elabftw\" or \"aruna\""))?;
            return self.resolve(experiment, keep);
        }
        if let Some(dataset) = input.get("resolve_create").and_then(Value::as_str) {
            return self.resolve_create(dataset, &input["experiment"]);
        }
        if input.get("reconcile").and_then(Value::as_bool) == Some(true) {
            self.full(true)?;
            return Ok(json!({ "reconcile": "started" }));
        }
        if let Some(experiment) = input.get("experiment").and_then(Value::as_u64) {
            let outcome = self.experiment(experiment, Mode::Manual)?;
            return shown(json!({ "experiment": experiment }), outcome);
        }
        let usage = "expected report, resolve, resolve_create, reconcile or experiment";
        Err(Fault::new(Kind::Invalid, usage))
    }

    fn check_bucket(&self) -> Result<()> {
        let uploads = self.settings.uploads();
        self.host.objects(&uploads).map_err(|fault| {
            let hint = "create the bucket on the install's receiver node";
            Fault::new(
                fault.kind,
                format!("{uploads} cannot be listed ({hint}): {}", fault.message),
            )
        })
    }

    /// Logs a link's error without failing the run; the link keeps its marks.
    fn guard(&self, result: Result<()>) {
        if let Err(fault) = result {
            self.host
                .log(&format!("a link was left for a later run: {fault}"));
        }
    }

    /// Whether the entry is the plugin's own last write, seen right after it.
    fn own_write(&self, entry: &Listed) -> Result<bool> {
        if entry.changed_by != self.settings.sync_user {
            return Ok(false);
        }
        let link = load::<Link>(self.host, &link_key(entry.id))?.map(|(_, link)| link);
        Ok(link.and_then(|link| link.written).as_deref() == Some(entry.modified.as_str()))
    }

    fn retry(&self, key: &str) -> Result<Outcome> {
        if let Some(experiment) = key.strip_prefix("link/").and_then(|id| id.parse().ok()) {
            return self.experiment(experiment, Mode::Retry);
        }
        match key.strip_prefix("dataset/") {
            Some(dataset) => self.dataset(dataset),
            None => Ok(Outcome::Skipped),
        }
    }

    /// Syncs one experiment's link, creating its dataset first when it has none, and returns
    /// the link's marks. Problems become marks.
    pub fn experiment(&self, id: u64, mode: Mode) -> Result<Outcome> {
        let mut record = Record::load(self.host, &link_key(id))?;
        let link = &mut record.link;
        if link
            .reservation
            .as_ref()
            .is_some_and(|held| held.busy(self.run, self.now))
        {
            return Ok(Outcome::Skipped);
        }
        if mode == Mode::Retry && link.retry_at.is_some_and(|at| at > self.now) {
            return Ok(Outcome::Skipped);
        }
        // A change to eLabFTW that ended before its write-back: the fields it sent take what was
        // sent as their base, so a later Aruna edit compares with it; a conflict stays one.
        let sent = link.reservation.take().and_then(|held| held.source);
        if let (Some(sent), Some(snapshot)) = (sent, link.snapshot.as_mut()) {
            snapshot.aruna.take(&sent.side, &sent.fields);
        }
        let kind = if link.dataset.is_none() {
            Reserved::Dataset
        } else {
            Reserved::Change
        };
        link.experiment = Some(id);
        link.reservation = Some(Reservation::new(kind, self.run, self.now));
        match record.save(self.host) {
            Err(fault) if fault.kind == Kind::Conflict => return Ok(Outcome::Skipped),
            other => other?,
        }
        let outcome = self.link(&mut record, id, mode);
        let link = &mut record.link;
        link.reservation = None;
        let spent = outcome
            .as_ref()
            .is_err_and(|fault| fault.kind == Kind::Spent);
        match outcome {
            Ok(()) => {
                for mark in [Mark::Failed, Mark::TooLarge, Mark::Unfinished] {
                    link.mark(mark, None);
                }
                link.retry_at = None;
            }
            // The retries of the next run go on from the progress this one saved.
            Err(fault) if fault.kind == Kind::Spent => {
                link.mark(Mark::Unfinished, Some(vec![fault.message]));
                link.retry_at = Some(self.now);
            }
            Err(fault) if fault.kind == Kind::Limit => {
                link.mark(Mark::TooLarge, Some(vec![fault.message]));
            }
            Err(fault) => link.fail(&fault.message, self.now, RETRY_MS),
        }
        if mode == Mode::Full && !spent {
            link.pass = self.pass.get();
        }
        let marks = link.marks.keys().copied().collect();
        record.save(self.host)?;
        Ok(if spent {
            Outcome::Spent
        } else {
            Outcome::Synced(marks)
        })
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
        let files = match read_files {
            true => Some(self.all_files(&dataset)?),
            false => None,
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
        if identified(&summary.json).is_none() && free_identifier(&summary.json) {
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
                    moved.aruna.take(&Side::of(&aruna), &decision.to_elab);
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

    /// All file entities of the dataset, a page per step of the budget.
    fn all_files(&self, dataset: &str) -> Result<Vec<Value>> {
        let mut files = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            self.step()?;
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
            self.step()?;
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
            held.source = Some(Sent {
                revision: revision.clone(),
                side: Side::of(aruna),
                fields: decision.to_elab.clone(),
            });
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
                        self.step()?;
                        elab::add_tag(self.host, id, tag)?;
                    }
                    let extra = (experiment.tags.iter())
                        .filter(|(tag, _)| !wanted.contains(*tag) && **tag != self.settings.tag);
                    for (_, tag) in extra {
                        self.step()?;
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
                    // eLabFTW casts each value of an ordinary update to a string, so the
                    // fields go as JSON text.
                    let fields = link_fields(self.settings, dataset, &links).to_string();
                    let merge = json!({ "metadatamerge": fields });
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
                self.step()?;
                let file = merge_file(stored.get(id.as_str()).copied(), id, facts);
                revision = self.host.put_file(dataset, &file, &revision)?;
            }
        }
        for id in current
            .keys()
            .filter(|id| id.starts_with(UPLOAD_PREFIX) && !wanted.contains_key(*id))
        {
            self.step()?;
            revision = self.host.remove_file(dataset, id, &revision)?;
        }
        Ok(revision)
    }

    /// A dataset below `meta/elabftw/` without a linked experiment: adopted when its identifier
    /// names one, else an experiment is created for it once.
    fn dataset(&self, dataset: &str) -> Result<Outcome> {
        let summary = self.host.summary(dataset)?;
        if let Some(experiment) = identified(&summary.json) {
            return self.adopt(dataset, experiment);
        }
        let mut record = Record::load(self.host, &dataset_key(dataset))?;
        if let Some(experiment) = record.link.experiment {
            return self.experiment(experiment, Mode::Event);
        }
        let held = record.link.reservation.clone();
        if held
            .as_ref()
            .is_some_and(|held| held.busy(self.run, self.now) && !held.unknown)
        {
            return Ok(Outcome::Skipped);
        }
        // An unknown outcome, or a create whose run stopped before it saved the outcome: the
        // `POST` may have landed, so only a found experiment or a person settles it.
        if let Some(mut held) = held {
            let found = elab::search(self.host, ID_FIELD, dataset)?;
            if let Some(experiment) = found.first() {
                return self.adopt(dataset, *experiment);
            }
            held.unknown = true;
            record.link.reservation = Some(held);
            let reason = "the experiment create has an unknown outcome; resolve it by hand";
            record
                .link
                .mark(Mark::UnknownCreate, Some(vec![reason.into()]));
            return self.saved(record);
        }
        // The files are read before the create is reserved, so a run out of budget leaves no
        // reservation that would look like a create with an unknown outcome.
        let files = match self.all_files(dataset) {
            Err(fault) if fault.kind == Kind::Spent => {
                record
                    .link
                    .mark(Mark::Unfinished, Some(vec![fault.message]));
                record.link.retry_at = Some(self.now);
                record.save(self.host)?;
                return Ok(Outcome::Spent);
            }
            other => other?,
        };
        record.link.dataset = Some(dataset.to_string());
        record.link.reservation = Some(Reservation::new(Reserved::Experiment, self.run, self.now));
        match record.save(self.host) {
            Err(fault) if fault.kind == Kind::Conflict => return Ok(Outcome::Skipped),
            other => other?,
        }
        let values = dataset_values(&summary.json, Some(&files), dataset, self.settings);
        let mut tags: Vec<&String> = values.tags.iter().collect();
        tags.push(&self.settings.tag);
        let links = values.links.clone().unwrap_or_default();
        // A dataset with only a plain text description starts the body from it.
        let text = match (values.body.text.as_str(), values.description.as_deref()) {
            ("", Some(plain)) if !plain.is_empty() => Body::from_plain(plain, values.body.format),
            _ => values.body.clone(),
        };
        let body = json!({
            "title": values.title,
            "body": text.text,
            "content_type": text.format.content_type(),
            "tags": tags,
            "metadata": link_fields(self.settings, dataset, &links).to_string(),
            "canread_base": BASE_TEAM,
            "canread": json!({ "teams": [], "teamgroups": [], "users": [] }).to_string(),
            "canwrite_base": BASE_USER,
            "canwrite": self.settings.canwrite().to_string(),
        });
        let key = format!("experiment/{dataset}/{}", record.link.attempt);
        match elab::create(self.host, body, key) {
            Ok(experiment) => {
                let mut link = Record::load(self.host, &link_key(experiment))?;
                link.link.experiment = Some(experiment);
                link.link.dataset = Some(dataset.to_string());
                let mut snapshot = Snapshot::agreed(&values);
                snapshot.revision = summary.revision.clone();
                link.link.snapshot = Some(snapshot.clone());
                link.save(self.host)?;
                record.link.reservation = None;
                record.link.experiment = Some(experiment);
                for mark in [Mark::UnknownCreate, Mark::Failed, Mark::Unfinished] {
                    record.link.mark(mark, None);
                }
                record.link.retry_at = None;
                record.save(self.host)?;
                let free = free_identifier(&summary.json);
                self.write_back(
                    &mut link,
                    (experiment, dataset, free),
                    (&values, &summary.revision),
                    snapshot,
                )?;
                Ok(Outcome::Synced(link.link.marks.keys().copied().collect()))
            }
            Err(fault) if fault.kind == Kind::Unknown => {
                if let Some(held) = record.link.reservation.as_mut() {
                    held.unknown = true;
                }
                record
                    .link
                    .mark(Mark::UnknownCreate, Some(vec![fault.message]));
                self.saved(record)
            }
            Err(fault) => {
                record.link.reservation = None;
                record.link.fail(&fault.message, self.now, RETRY_MS);
                self.saved(record)
            }
        }
    }

    /// Saves a dataset's record and answers its marks.
    fn saved(&self, mut record: Record) -> Result<Outcome> {
        record.save(self.host)?;
        Ok(Outcome::Synced(record.link.marks.keys().copied().collect()))
    }

    /// After an experiment create: eLabFTW's stored body and the identifier go to the dataset,
    /// against the revision the experiment was made from.
    fn write_back(
        &self,
        link: &mut Record,
        (experiment, dataset, free): (u64, &str, bool),
        (sent, revision): (&Values, &String),
        mut snapshot: Snapshot,
    ) -> Result<()> {
        let after = elab::experiment(self.host, experiment)?;
        let after =
            after.ok_or_else(|| Fault::new(Kind::NotFound, "the new experiment is gone"))?;
        let stored = after.values(None, self.settings);
        let mut target = sent.clone();
        copy(Field::Body, &stored, &mut target);
        let mut patch = dataset_patch(&target, &[Field::Body]);
        if free {
            patch["identifier"] = identifier(experiment, self.settings);
        }
        snapshot.elab.update(&stored, &[]);
        match self.host.patch(dataset, &patch, revision) {
            Ok(next) => {
                snapshot.aruna.update(&target, &[]);
                snapshot.revision = next;
            }
            // An Aruna edit since the read compares with what was sent, and the stored body
            // still goes back in the next sync.
            Err(fault) if fault.kind == Kind::Conflict => snapshot.pending = true,
            Err(fault) => return Err(fault),
        }
        snapshot.modified = after.modified.clone();
        link.link.snapshot = Some(snapshot);
        link.link.written = Some(after.modified);
        link.save(self.host)
    }

    /// Links a dataset to an existing experiment, as a found lookup or a person decides.
    fn adopt(&self, dataset: &str, experiment: u64) -> Result<Outcome> {
        let mut link = Record::load(self.host, &link_key(experiment))?;
        if link.link.dataset.as_deref() != Some(dataset) {
            link.link.experiment = Some(experiment);
            link.link.dataset = Some(dataset.to_string());
            link.link.snapshot = None;
            link.save(self.host)?;
        }
        let mut pointer = Record::load(self.host, &dataset_key(dataset))?;
        pointer.link.reservation = None;
        pointer.link.experiment = Some(experiment);
        pointer.link.mark(Mark::UnknownCreate, None);
        pointer.link.mark(Mark::Failed, None);
        pointer.save(self.host)?;
        self.experiment(experiment, Mode::Manual)
    }

    /// Settles a conflict or drift: the marked fields take the values of `keep` next.
    fn resolve(&self, experiment: u64, keep: Origin) -> Result<Value> {
        let mut record = Record::load(self.host, &link_key(experiment))?;
        let marked = [Mark::Conflict, Mark::OutOfSync]
            .iter()
            .filter_map(|mark| record.link.marks.get(mark));
        let names = marked
            .flatten()
            .filter_map(|detail| detail.split(' ').next());
        let fields: BTreeSet<Field> = names.filter_map(Field::named).collect();
        record.link.force = Some((keep, fields.into_iter().collect()));
        record.link.mark(Mark::Conflict, None);
        record.link.mark(Mark::OutOfSync, None);
        record.save(self.host)?;
        let outcome = self.experiment(experiment, Mode::Manual)?;
        shown(json!({ "resolved": experiment }), outcome)
    }

    /// Settles an experiment create with an unknown outcome: the experiment a person found, or
    /// `"absent"` to allow a new create under a new key.
    fn resolve_create(&self, dataset: &str, experiment: &Value) -> Result<Value> {
        if let Some(experiment) = experiment.as_u64() {
            let outcome = self.adopt(dataset, experiment)?;
            return shown(json!({ "adopted": experiment }), outcome);
        }
        if experiment.as_str() != Some("absent") {
            return Err(Fault::new(
                Kind::Invalid,
                "experiment is an id or \"absent\"",
            ));
        }
        let mut record = Record::load(self.host, &dataset_key(dataset))?;
        record.link.reservation = None;
        record.link.attempt += 1;
        record.link.mark(Mark::UnknownCreate, None);
        record.save(self.host)?;
        let outcome = self.dataset(dataset)?;
        shown(json!({ "created_again": dataset }), outcome)
    }

    /// The full reconciliation, as far as the run's budget goes; `start` begins a new pass now.
    fn full(&self, start: bool) -> Result<()> {
        let (version, mut pass) = match load::<Pass>(self.host, "full")? {
            Some((version, pass)) => (Some(version), pass),
            None => (None, Pass::default()),
        };
        if start || pass.due(self.now) {
            pass.start(self.now);
        }
        self.pass.set(pass.id);
        let mut checked = 0;
        match pass.phase {
            Phase::Idle => return Ok(()),
            _ if !self.room(self.budget.step) => {}
            Phase::Experiments => {
                let tag = &self.settings.tag;
                let entries = elab::list(self.host, tag, Order::Id, pass.offset)?;
                let total = entries.len();
                for entry in entries {
                    if !self.room(self.budget.link) {
                        break;
                    }
                    if !self.count(&mut pass, self.experiment(entry.id, Mode::Full)) {
                        break;
                    }
                    pass.offset += 1;
                    checked += 1;
                }
                if checked == total && total < elab::PAGE as usize {
                    (pass.phase, pass.offset) = (Phase::Links, 0);
                }
            }
            Phase::Links => {
                let page = pass.offset / LINK_PAGE as u64;
                let (ids, more) = link_page(self.host, page)?;
                let start = (pass.offset % LINK_PAGE as u64) as usize;
                for id in ids.iter().skip(start) {
                    if !self.room(self.budget.link) {
                        break;
                    }
                    let link = load::<Link>(self.host, &link_key(*id))?.map(|(_, link)| link);
                    let unchecked = link.is_some_and(|link| link.pass != pass.id);
                    if unchecked && !self.count(&mut pass, self.experiment(*id, Mode::Full)) {
                        break;
                    }
                    pass.offset += 1;
                }
                let next = (page + 1) * LINK_PAGE as u64;
                let page_done =
                    pass.offset >= next || (pass.offset % LINK_PAGE as u64) as usize >= ids.len();
                match (page_done, more) {
                    (true, true) => pass.offset = next,
                    (true, false) => pass.phase = Phase::Idle,
                    (false, _) => {}
                }
                if pass.phase == Phase::Idle {
                    pass.finished = self.now;
                    self.host.log(&pass.summary());
                }
            }
        }
        match store(self.host, "full", &pass, version) {
            Err(fault) if fault.kind == Kind::Conflict => Ok(()),
            other => other.map(drop),
        }
    }

    /// Adds a checked link's marks to the pass; `false` when another run holds the link or
    /// the budget ran out in it, so the pass goes on there next time.
    fn count(&self, pass: &mut Pass, outcome: Result<Outcome>) -> bool {
        match outcome {
            Ok(Outcome::Skipped | Outcome::Spent) => return false,
            Ok(Outcome::Synced(marks)) => {
                for mark in marks {
                    *pass.counts.entry(mark).or_default() += 1;
                }
            }
            Err(fault) => self.guard(Err(fault)),
        }
        true
    }
}

/// A manual run's answer: `fields` with the link's marks, or `unfinished` when a later run
/// goes on with it.
fn shown(mut fields: Value, outcome: Outcome) -> Result<Value> {
    match outcome {
        Outcome::Synced(marks) => fields["marks"] = json!(marks),
        Outcome::Spent => fields["unfinished"] = json!(true),
        Outcome::Skipped => {
            return Err(Fault::new(Kind::Unavailable, "another run holds the link"));
        }
    }
    Ok(fields)
}

/// Whether the dataset's `identifier` can take the experiment's: absent, or a plain object
/// that a merge patch extends; another kind, such as a DOI, stays as it is.
fn free_identifier(json: &Value) -> bool {
    json.get("identifier").is_none_or(Value::is_object)
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
