//! Probes the candidates of each File entity until one opens, and records why the others failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::jobs::JobEffect;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::ReasonCode;

use super::open::OpenCandidateOperation;
use super::resolve::ExportCandidate;
use super::sources::{
    AliasState, HashCandidatesOperation, HashFound, learn_probe_hash, local_bucket, stop_failure,
};
use super::{CandidateOpen, ExportEntity, ExportFailure, OpenStatus, ProbedEntry, SourceScope};
use crate::jobs::import::{job_call, stops_of};
use crate::replication::bao_read::BaoReadOutput;

/// Candidates that failed while the archive was written, by entity and candidate.
pub type CandidateFailures = BTreeMap<usize, BTreeMap<usize, OpenStatus>>;

/// The entities after a probe and the copy chosen for each included one.
#[derive(Debug, PartialEq)]
pub struct Probed {
    pub entities: Vec<ExportEntity>,
    pub entries: Vec<ProbedEntry>,
    pub aliases: AliasState,
}

/// What the candidates of the running entity showed so far.
#[derive(Debug, Default, PartialEq)]
struct Findings {
    denied: bool,
    missing: bool,
    offline: bool,
    corrupt: bool,
    unheld: Option<String>,
}

impl Findings {
    fn note(&mut self, status: OpenStatus, candidate: Option<&ExportCandidate>) {
        match status {
            OpenStatus::Denied => self.denied = true,
            OpenStatus::NotHolder => self.unheld = local_bucket(candidate),
            OpenStatus::Missing => self.missing = true,
            OpenStatus::Offline => self.offline = true,
            OpenStatus::Corrupt => self.corrupt = true,
        }
    }
}

#[derive(Debug, PartialEq)]
enum ProbeStep {
    Stops,
    Open,
    Extend(ProbedEntry),
}

/// Finds the first candidate of each open entity that can be read. An entity without one is left
/// out, unless a corrupt copy or a missing bucket key must fail the export.
#[derive(Debug, PartialEq)]
pub struct ProbeSourcesOperation {
    scope: SourceScope,
    entities: Vec<ExportEntity>,
    failures: CandidateFailures,
    aliases: AliasState,
    entries: Vec<ProbedEntry>,
    index: usize,
    candidates: Vec<ExportCandidate>,
    position: usize,
    findings: Findings,
    step: ProbeStep,
}

type ProbeTurn = Result<Next<Probed>, ExportFailure>;

impl ProbeSourcesOperation {
    pub fn new(
        scope: SourceScope,
        entities: Vec<ExportEntity>,
        failures: CandidateFailures,
        aliases: AliasState,
    ) -> Self {
        Self {
            scope,
            entities,
            failures,
            aliases,
            entries: Vec::new(),
            index: 0,
            candidates: Vec::new(),
            position: 0,
            findings: Findings::default(),
            step: ProbeStep::Stops,
        }
    }

    /// Starts the next entity that is still open.
    fn next_entity(&mut self) -> ProbeTurn {
        while (self.entities.get(self.index)).is_some_and(|entity| entity.omission.is_some()) {
            self.index += 1;
        }
        let Some(entity) = self.entities.get(self.index) else {
            return Ok(Next::Done(Probed {
                entities: std::mem::take(&mut self.entities),
                entries: std::mem::take(&mut self.entries),
                aliases: std::mem::take(&mut self.aliases),
            }));
        };
        self.candidates = entity.candidates.clone();
        self.position = 0;
        self.findings = Findings::default();
        let failed = self.failures.get(&self.index).into_iter().flatten();
        for (candidate, status) in failed {
            self.findings.note(*status, self.candidates.get(*candidate));
        }
        self.next_candidate()
    }

    fn next_candidate(&mut self) -> ProbeTurn {
        let failed = self.failures.get(&self.index);
        while failed.is_some_and(|failed| failed.contains_key(&self.position)) {
            self.position += 1;
        }
        if self.position >= self.candidates.len() {
            return self.conclude();
        }
        self.step = ProbeStep::Stops;
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }

    /// No candidate opened: the entity is left out with the strongest reason found.
    fn conclude(&mut self) -> ProbeTurn {
        let findings = std::mem::take(&mut self.findings);
        if findings.corrupt {
            return Err(ExportFailure::Retryable(
                "payload integrity check failed".to_string(),
            ));
        }
        // No copy opened and one of them needs a bucket key the caller lacks.
        if let Some(bucket) = findings.unheld {
            let refused = aruna_core::transfer::GrantError::NotHolder(bucket);
            return Err(ExportFailure::Permanent(refused.to_string()));
        }
        let (code, message) = match (findings.denied, findings.offline, findings.missing) {
            (true, _, _) => (ReasonCode::Denied, "all payload candidates denied READ"),
            (_, true, _) => (ReasonCode::Offline, "all payload candidates are offline"),
            (_, _, true) => (ReasonCode::Missing, "all payload candidates are missing"),
            _ => (ReasonCode::Missing, "no payload candidate is available"),
        };
        let entity = &mut self.entities[self.index];
        entity.zip_path = None;
        entity.report_source = None;
        entity.resolved_version = None;
        entity.path_synthesized = false;
        entity.omission = Some(code);
        entity.message = Some(message.to_string());
        self.index += 1;
        self.next_entity()
    }

    fn select(&mut self, entry: ProbedEntry) -> ProbeTurn {
        self.entries.push(entry);
        self.index += 1;
        self.next_entity()
    }

    fn opened(&mut self, size: u64, blake3: [u8; 32]) -> ProbeTurn {
        let candidate = &self.candidates[self.position];
        let entry = ProbedEntry {
            entity_index: self.index,
            candidate_index: self.position,
            size,
            hash: blake3,
            report_source: candidate.report_source,
            resolved_version: candidate.resolved_version,
        };
        let version = candidate.resolved_version;
        let entity = &mut self.entities[self.index];
        if !learn_probe_hash(entity, self.position, blake3) {
            return self.select(entry);
        }
        let lookup = HashCandidatesOperation::new(
            self.scope.clone(),
            blake3,
            version,
            std::mem::take(&mut entity.candidates),
            std::mem::take(&mut self.aliases),
        );
        self.step = ProbeStep::Extend(entry);
        Ok(Next::Call(IoCall::child(lookup)))
    }
}

impl Operation for ProbeSourcesOperation {
    type Output = Probed;
    type Error = ExportFailure;

    fn first(&mut self) -> ProbeTurn {
        self.next_entity()
    }

    fn next(&mut self, result: IoResult) -> ProbeTurn {
        match std::mem::replace(&mut self.step, ProbeStep::Open) {
            ProbeStep::Stops => {
                if let Some(stopped) = stop_failure(stops_of(result)?) {
                    return Err(stopped);
                }
                let candidate = self.candidates[self.position].clone();
                let open = OpenCandidateOperation::new(self.scope.clone(), candidate, true);
                self.step = ProbeStep::Open;
                Ok(Next::Call(IoCall::child(open)))
            }
            ProbeStep::Open => match result.child::<CandidateOpen, ExportFailure>()?? {
                CandidateOpen::Opened(BaoReadOutput::Metadata { size, blake3, .. }) => {
                    self.opened(size, blake3)
                }
                CandidateOpen::Opened(BaoReadOutput::Stream { .. }) => {
                    Err(ExportFailure::Retryable(
                        "source probe unexpectedly opened a stream".to_string(),
                    ))
                }
                CandidateOpen::Status(status) => {
                    let candidate = self.candidates.get(self.position);
                    self.findings.note(status, candidate);
                    self.position += 1;
                    self.next_candidate()
                }
            },
            ProbeStep::Extend(entry) => {
                let found = result.child::<HashFound, ExportFailure>()??;
                self.entities[self.index].candidates = found.candidates;
                self.aliases = found.aliases;
                self.select(entry)
            }
        }
    }
}

#[cfg(test)]
#[path = "probe_tests.rs"]
mod tests;
