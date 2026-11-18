//! Writes one ARC snapshot and folds covered records into a checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{
    GitChange, GitCheckpoint, GitEffect, GitEvent, GitPack, GitRecord, GitSnapshot, GitStatus,
    LinkedObject, RefUpdate, STATUS, StoredObject, ZERO_OID,
};
use aruna_core::git_state::Projection;
use aruna_core::keyspaces::{COMMIT_MESSAGE_KEYSPACE, EVENT_LOG_KEYSPACE};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::operation::Operation;
use aruna_core::repo_layout::{ARUNA_FILE, CRATE_FILE};
use aruna_core::storage_entries::event_log_key;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::NodeRealm;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use serde_json::Value;
use ulid::Ulid;

use super::calls::{StageTimes, git_call, git_event};
use super::changes::summary;
use super::linked::linked;
use super::project::author;
use super::publish::PublishRecordOperation;
use super::versions::{copied, plain};
use super::{GitError, objects, publish, records};

type Document = MetadataRegistryRecord;

/// A subject that says what changed since the last snapshot, for metadata without a message.
fn subject(
    document: &Document,
    refs: &BTreeMap<String, String>,
    jsonld: &str,
    user: UserId,
) -> SubjectOperation {
    SubjectOperation {
        document_id: document.document_id,
        commit: refs.get("refs/heads/aruna").cloned(),
        user,
        after: serde_json::from_str(jsonld).map_or(Value::Null, copied),
        paths: VecDeque::from([ARUNA_FILE, CRATE_FILE]),
    }
}

/// Reads the metadata a snapshot commit stores as the file itself: the ARC graph file, else
/// the RO-Crate. The full version view also adds file entities, which a subject never needs.
#[derive(Debug, PartialEq)]
struct SubjectOperation {
    document_id: Ulid,
    commit: Option<String>,
    user: UserId,
    after: Value,
    paths: VecDeque<&'static str>,
}

impl SubjectOperation {
    fn next_path(&mut self) -> Result<Next<String>, GitError> {
        let path = self.paths.pop_front();
        let (Some(path), Some(commit)) = (path, self.commit.as_ref()) else {
            return Ok(Next::Done(summary(None, &self.after)));
        };
        let effect = GitEffect::ReadFile {
            document_id: self.document_id,
            revision: commit.clone(),
            path: path.to_string(),
        };
        Ok(Next::Call(git_call(effect, self.user)))
    }
}

impl Operation for SubjectOperation {
    type Output = String;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<String>, GitError> {
        self.next_path()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, GitError> {
        let before = match git_event(result) {
            Ok(GitEvent::File(None)) => return self.next_path(),
            Ok(GitEvent::File(Some(bytes))) => serde_json::from_slice(&bytes).ok().map(copied),
            _ => None,
        };
        Ok(Next::Done(summary(before.as_ref(), &self.after)))
    }
}

/// What the pending call of generating a snapshot answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenerateStep {
    Event,
    Message,
    Subject,
    Linked,
    Generate,
    Pack,
    Publish,
    Mark,
    Status,
}

/// Generates the snapshot of `source` from the graph and publishes it as a Git record. A
/// conversion failure is stored as the document's status and is no error.
pub(super) fn generate(
    ids: NodeRealm,
    document: &Document,
    projection: &Projection,
    (event_id, jsonld): (Ulid, String),
    digest: Option<[u8; 32]>,
) -> GenerateOperation {
    GenerateOperation {
        ids,
        document: document.clone(),
        refs: projection.state.refs.clone(),
        event_id,
        jsonld,
        digest,
        step: GenerateStep::Event,
        user: UserId::nil(document.realm_id),
        occurred_at_ms: document.updated_at_ms,
        message: String::new(),
        lfs: Vec::new(),
        aruna: String::new(),
        main: None,
        marked: None,
        stages: StageTimes::default(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct GenerateOperation {
    ids: NodeRealm,
    document: Document,
    refs: BTreeMap<String, String>,
    event_id: Ulid,
    jsonld: String,
    digest: Option<[u8; 32]>,
    step: GenerateStep,
    user: UserId,
    occurred_at_ms: u64,
    message: String,
    lfs: Vec<StoredObject>,
    aruna: String,
    main: Option<String>,
    marked: Option<GitPack>,
    stages: StageTimes,
}

type GenerateTurn = Result<Next<()>, GitError>;

impl GenerateOperation {
    fn id(&self) -> Ulid {
        self.document.document_id
    }

    fn event_key(&self) -> Vec<u8> {
        event_log_key(self.id(), self.event_id).to_vec()
    }

    fn find_links(&mut self) -> GenerateTurn {
        // Linked objects are read in the realm that holds the document, not the author's home.
        let linker = AuthContext {
            realm_id: self.document.realm_id,
            ..author(self.user)
        };
        self.step = GenerateStep::Linked;
        self.stages.start("git_linked");
        let find = linked(self.ids, &self.document, &self.jsonld, &linker);
        Ok(Next::Call(IoCall::child(find)))
    }

    fn store_status(&mut self, commit: Option<String>, error: Option<String>) -> GenerateTurn {
        let status = GitStatus {
            event_id: self.event_id,
            commit,
            error,
        };
        self.step = GenerateStep::Status;
        let key = self.id().to_bytes().to_vec();
        Ok(Next::Call(IoCall::child(records::save(
            STATUS, key, &status,
        ))))
    }

    fn store_written(&mut self) -> GenerateTurn {
        self.stages.start("git_status");
        self.store_status(Some(self.aruna.clone()), None)
    }

    fn publish(&mut self, pack: Option<Bytes>) -> GenerateTurn {
        let described = pack.as_ref().map(objects::describe_pack).transpose()?;
        let refs = &self.refs;
        let update = |name: &str, new: String| RefUpdate {
            name: name.into(),
            old: refs.get(name).cloned().unwrap_or_else(|| ZERO_OID.into()),
            new,
        };
        let main = self.main.take();
        let mut made = vec![self.aruna.clone()];
        made.extend(main.iter().cloned());
        let mut updates = vec![update("refs/heads/aruna", self.aruna.clone())];
        updates.extend(main.map(|main| update("refs/heads/main", main)));
        updates.retain(|update| update.old != update.new);
        made.retain(|commit| updates.iter().any(|update| update.new == *commit));
        self.marked = described.clone();
        let change = GitChange::Objects {
            pack: described,
            refs: updates,
            lfs: std::mem::take(&mut self.lfs),
            revision: Some(self.event_id),
            digest: self.digest,
            made,
        };
        let node = self.ids.map(|(node_id, _)| node_id);
        let extras = (pack, Vec::new());
        let record = publish::publish_with(node, &self.document, self.user, change, extras);
        self.step = GenerateStep::Publish;
        self.stages.start("git_publish");
        Ok(Next::Call(IoCall::child(record)))
    }

    fn generated(&mut self, aruna: String, main: Option<String>) -> GenerateTurn {
        let unchanged = self.refs.get("refs/heads/aruna") == Some(&aruna) && main.is_none();
        self.aruna = aruna;
        self.main = main;
        if unchanged {
            return self.publish(None);
        }
        let mut include = vec![self.aruna.clone()];
        include.extend(self.main.iter().cloned());
        let effect = GitEffect::Pack {
            document_id: self.id(),
            include,
            exclude: self.refs.values().cloned().collect(),
        };
        self.step = GenerateStep::Pack;
        Ok(Next::Call(git_call(effect, self.user)))
    }
}

impl Operation for GenerateOperation {
    type Output = ();
    type Error = GitError;

    fn abort(&mut self) -> Vec<IoCall> {
        self.stages.report()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.stages.stamp(now_ms);
    }

    fn first(&mut self) -> GenerateTurn {
        let load = records::load::<MetadataEventRecord>(EVENT_LOG_KEYSPACE, self.event_key());
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> GenerateTurn {
        self.stages.stop();
        match self.step {
            GenerateStep::Event => {
                if let Some(event) = result.child::<Option<MetadataEventRecord>, GitError>()?? {
                    self.user = event.user_id;
                    self.occurred_at_ms = event.occurred_at_ms;
                }
                self.step = GenerateStep::Message;
                let load = records::load::<String>(COMMIT_MESSAGE_KEYSPACE, self.event_key());
                Ok(Next::Call(IoCall::child(load)))
            }
            GenerateStep::Message => {
                // Trailer lines stay reserved for Aruna, so an author cannot forge a trusted one.
                let message = result.child::<Option<String>, GitError>()??;
                let message = message.map(|message| plain(&message));
                match message.filter(|message| !message.is_empty()) {
                    Some(message) => {
                        self.message = message;
                        self.find_links()
                    }
                    None => {
                        self.step = GenerateStep::Subject;
                        self.stages.start("git_subject");
                        let read = subject(&self.document, &self.refs, &self.jsonld, self.user);
                        Ok(Next::Call(IoCall::child(read)))
                    }
                }
            }
            GenerateStep::Subject => {
                self.message = result.child::<String, GitError>()??;
                self.find_links()
            }
            GenerateStep::Linked => {
                let objects = result.child::<Vec<LinkedObject>, GitError>()??;
                self.lfs = objects.iter().map(|linked| linked.object.clone()).collect();
                let effect = GitEffect::Generate {
                    snapshot: GitSnapshot {
                        document_id: self.id(),
                        event_id: self.event_id,
                        occurred_at_ms: self.occurred_at_ms,
                        jsonld: std::mem::take(&mut self.jsonld),
                        objects,
                        message: Some(std::mem::take(&mut self.message)),
                    },
                    refs: self.refs.clone(),
                };
                self.step = GenerateStep::Generate;
                Ok(Next::Call(git_call(effect, self.user)))
            }
            GenerateStep::Generate => match git_event(result)? {
                GitEvent::Generated { aruna, main } => self.generated(aruna, main),
                GitEvent::GenerateFailed(error) => {
                    let commit = self.refs.get("refs/heads/aruna").cloned();
                    self.store_status(commit, Some(error))
                }
                _ => Err(GitError::Unavailable),
            },
            GenerateStep::Pack => {
                let GitEvent::Packed(pack) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                self.publish(Some(pack))
            }
            GenerateStep::Publish => {
                result.child::<GitRecord, GitError>()??;
                match self.marked.take() {
                    Some(pack) => {
                        self.step = GenerateStep::Mark;
                        let mark = objects::mark_imported((self.id(), self.user), &pack);
                        Ok(Next::Call(IoCall::child(mark)))
                    }
                    None => self.store_written(),
                }
            }
            GenerateStep::Mark => {
                // A failure only costs one more index run in the cache.
                result.child::<(), IoError>()?.ok();
                self.store_written()
            }
            GenerateStep::Status => {
                result.child::<(), GitError>()??;
                Ok(self.stages.finish((), Vec::new()))
            }
        }
    }
}

/// Folds the records covered so far into one checkpoint so history never hits its cap.
pub(super) fn checkpoint(
    ids: NodeRealm,
    document: &Document,
    projection: &Projection,
) -> PublishRecordOperation {
    let state = &projection.state;
    let owner =
        (projection.records.last()).map_or(UserId::nil(document.realm_id), |record| record.user_id);
    let change = GitChange::Checkpoint(Box::new(GitCheckpoint {
        previous: state.checkpoint,
        packs: state.new_packs.clone(),
        made: state.new_made.clone(),
        digest: state.digest,
        refs: state.refs.clone().into_iter().collect(),
        lfs: state.new_lfs.clone(),
        locks: state.locks.values().cloned().collect(),
        waiting: state.waiting.clone(),
        released: state.new_released.clone(),
        revision: state.revision,
        covered: state.coverable(),
    }));
    publish::publish(ids.map(|(node_id, _)| node_id), document, owner, change)
}

#[cfg(test)]
#[path = "generate_tests.rs"]
mod tests;
