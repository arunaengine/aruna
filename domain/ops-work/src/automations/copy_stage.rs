//! Intent-first `copy` stage executor with bucket auth, quota, distribution hint and recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{CopiedFile, Firing};
use aruna_core::automation::glob::{expand_copy, format_date, glob_match};
use aruna_core::automation::keys::copy_plan_key;
use aruna_core::automation::record::{Automation, Spec, StageKind};
use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::permission_path::permission_pattern_matches;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::hint::mint_version;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::types::GroupId;
use aruna_ops_data::s3::object::get::GetObjectError;
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_ops_data::s3::object::put::PutObjectError;
use aruna_ops_data::transfer::copy::{
    CopyObjectError, CopyObjectInput, CopyObjectOperation, CopyReferences, CopyResultData,
    CopySourceConditions,
};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use byteview::ByteView;
use ulid::Ulid;

use super::limits::CountUsageOperation;
use super::{AutoError, allowed, authorize_path, automation_auth_for};
use crate::plugins::intents::{Begun, begin_in, row_key_in, settled_row_in, unknown_row_in};
use crate::plugins::rows::{StoreError, delete_call};

pub(crate) fn stage_intent_id(firing_id: Ulid, stage: &str) -> Ulid {
    let mut h = blake3::Hasher::new();
    h.update(&firing_id.to_bytes());
    h.update(stage.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&h.finalize().as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

pub(crate) fn call_intent(ordinal: u32, parts: &[&[u8]]) -> CallIntent {
    let mut h = blake3::Hasher::new();
    h.update(&ordinal.to_be_bytes());
    for part in parts {
        h.update(&(part.len() as u64).to_be_bytes());
        h.update(part);
    }
    CallIntent {
        ordinal,
        digest: *h.finalize().as_bytes(),
    }
}

/// Source length matching `CopyJobOperation`: local blob size or reference observation size.
pub(super) fn source_length(head: &HeadObjectResult) -> Option<u64> {
    head.location
        .as_ref()
        .map(|l| l.blob_size)
        .or_else(|| head.source_metadata.as_ref().map(|m| m.content_length))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageOutcome {
    Progress(CopiedFile),
    CopiedAll,
    Dataset(Ulid),
    Notified(u32),
    Failed(String),
    NeedsAttention(String, Option<CopiedFile>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CopyItem {
    ordinal: u32,
    src_bucket: String,
    src_key: String,
    src_ver: Ulid,
    digest: Option<String>,
}

fn collect_sources(firing: &Firing, from: &str, filter: Option<&str>) -> Vec<CopyItem> {
    let Some(stage) = firing.stages.iter().find(|s| s.name == from) else {
        return Vec::new();
    };
    let mut items = Vec::new();
    if !stage.receipt.copied.is_empty() {
        for (i, file) in stage.receipt.copied.iter().enumerate() {
            let name = file.dest_key.rsplit('/').next().unwrap_or(&file.dest_key);
            if filter.is_none_or(|p| glob_match(p, name)) {
                items.push(CopyItem {
                    ordinal: i as u32,
                    src_bucket: file.dest_bucket.clone(),
                    src_key: file.dest_key.clone(),
                    src_ver: file.dest_version,
                    digest: None,
                });
            }
        }
        return items;
    }
    for (i, file) in stage.receipt.outputs.iter().enumerate() {
        let (Some(bucket), Some(key), Some(ver)) = (
            file.bucket.as_ref(),
            file.key.as_ref(),
            file.version_id
                .as_deref()
                .and_then(|v| Ulid::from_string(v).ok()),
        ) else {
            continue;
        };
        let name = key.rsplit('/').next().unwrap_or(key);
        if filter.is_none_or(|p| glob_match(p, &file.name) || glob_match(p, name)) {
            items.push(CopyItem {
                ordinal: i as u32,
                src_bucket: bucket.clone(),
                src_key: key.clone(),
                src_ver: ver,
                digest: file.digest.clone(),
            });
        }
    }
    items
}

pub(super) fn source_count(firing: &Firing, from: &str, filter: Option<&str>) -> usize {
    collect_sources(firing, from, filter).len()
}

#[derive(Debug, PartialEq)]
enum CopyStep {
    Begin,
    ReadPlan(String),
    SourceBucket,
    DestBucket,
    Head,
    AuthSource(String, u64, String),
    AuthDest(String, u64),
    Config(String, u64),
    Count(String, u64),
    WritePlan(Box<CopiedFile>),
    Copy(Box<CopiedFile>),
    Settle(Box<CopiedFile>),
    CleanPlan(Box<CopiedFile>),
    Drop(String),
    MarkUnknown(String, Box<CopiedFile>),
}

/// Copies the next uncommitted file of a `copy` stage with intent-first durability, bucket owner
/// authorization, group quota enforcement, distribution-hinted versions and recovery plan rows.
#[derive(Debug, PartialEq)]
pub struct CopyStageOp {
    auto: Automation,
    spec: Spec,
    firing_id: Ulid,
    stage_id: Ulid,
    dest_bucket: String,
    dest_template: String,
    allow_downgrade: bool,
    local: (NodeId, RealmId),
    auth: Option<AuthContext>,
    src_group: Option<GroupId>,
    dest_info: Option<BucketInfo>,
    quota_ceiling: Option<u64>,
    item: Option<CopyItem>,
    intent: Option<CallIntent>,
    ids: IdMint,
    now_ms: u64,
    step: CopyStep,
}

impl CopyStageOp {
    pub fn new(
        auto: Automation,
        firing: Firing,
        stage_idx: usize,
        local: (NodeId, RealmId),
    ) -> Result<Self, AutoError> {
        let (stage_name, from_stage, file, dest_bucket, dest_template, allow_downgrade) =
            match firing.spec.stages.get(stage_idx) {
                Some(stage) => match &stage.kind {
                    StageKind::Copy {
                        from_stage,
                        file,
                        dest_bucket,
                        dest_template,
                        allow_downgrade,
                    } => (
                        stage.name.clone(),
                        from_stage.clone(),
                        file.clone(),
                        dest_bucket.clone(),
                        dest_template.clone(),
                        *allow_downgrade,
                    ),
                    _ => return Err(AutoError::Invalid("stage is not a copy stage".into())),
                },
                None => return Err(AutoError::Invalid("missing copy stage".into())),
            };
        let done = firing.stages[stage_idx].receipt.copied.len();
        let item = collect_sources(&firing, &from_stage, file.as_deref())
            .into_iter()
            .nth(done);
        let stage_id = stage_intent_id(firing.firing_id, &stage_name);
        Ok(Self {
            auto,
            spec: firing.spec,
            firing_id: firing.firing_id,
            stage_id,
            dest_bucket,
            dest_template,
            allow_downgrade,
            local,
            auth: None,
            src_group: None,
            dest_info: None,
            quota_ceiling: None,
            item,
            intent: None,
            ids: IdMint::default(),
            now_ms: 0,
            step: CopyStep::Begin,
        })
    }

    fn drop_intent(&mut self, intent: &CallIntent, msg: String) -> Next<StageOutcome> {
        let (space, key) = row_key_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, intent);
        let drops = vec![
            (space.to_string(), key.into()),
            (
                AUTOMATION_RUNS_KEYSPACE.to_string(),
                copy_plan_key(self.stage_id, intent.ordinal),
            ),
        ];
        self.step = CopyStep::Drop(msg);
        Next::Call(IoCall::Delete(drops))
    }
}

impl Operation for CopyStageOp {
    type Output = StageOutcome;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<StageOutcome>, AutoError> {
        let Some(item) = &self.item else {
            return Ok(Next::Done(StageOutcome::CopiedAll));
        };
        let auth = automation_auth_for(&self.auto, &self.spec, self.local.1)
            .ok_or_else(|| AutoError::Conflict("automation has no active account".into()))?;
        self.auth = Some(auth);
        let intent = call_intent(
            item.ordinal,
            &[
                item.src_bucket.as_bytes(),
                item.src_key.as_bytes(),
                &item.src_ver.to_bytes(),
                self.dest_bucket.as_bytes(),
                self.dest_template.as_bytes(),
            ],
        );
        self.intent = Some(intent.clone());
        let begin = begin_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, intent);
        Ok(Next::Call(IoCall::child(begin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StageOutcome>, AutoError> {
        let item = self.item.clone().ok_or(IoError::Unfinished)?;
        let intent = self.intent.clone().ok_or(IoError::Unfinished)?;
        match std::mem::replace(&mut self.step, CopyStep::Begin) {
            CopyStep::Begin => match result
                .child::<Begun, StoreError>()?
                .map_err(|e| AutoError::Storage(e.detail()))?
            {
                Begun::Fresh => {
                    let read = GetBucketOperation::new(item.src_bucket);
                    self.step = CopyStep::SourceBucket;
                    Ok(Next::Call(IoCall::child(read)))
                }
                Begun::Settled(reply) => {
                    let copied: CopiedFile = serde_json::from_str(&reply)
                        .map_err(|err| AutoError::Storage(err.to_string()))?;
                    Ok(Next::Done(StageOutcome::Progress(copied)))
                }
                Begun::Changed => Ok(Next::Done(StageOutcome::NeedsAttention(
                    "copy arguments changed across attempts".into(),
                    None,
                ))),
                Begun::Unknown(reason) => {
                    self.step = CopyStep::ReadPlan(reason);
                    Ok(Next::Call(IoCall::Read(vec![(
                        AUTOMATION_RUNS_KEYSPACE.to_string(),
                        copy_plan_key(self.stage_id, item.ordinal),
                    )])))
                }
            },
            CopyStep::ReadPlan(reason) => {
                let planned = result
                    .value()?
                    .and_then(|bytes| postcard::from_bytes::<CopiedFile>(&bytes).ok());
                Ok(Next::Done(StageOutcome::NeedsAttention(reason, planned)))
            }
            CopyStep::SourceBucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => {
                    self.src_group = Some(info.group_id);
                    let read = GetBucketOperation::new(self.dest_bucket.clone());
                    self.step = CopyStep::DestBucket;
                    Ok(Next::Call(IoCall::child(read)))
                }
                Err(err) => Ok(self.drop_intent(&intent, format!("source bucket failed: {err}"))),
            },
            CopyStep::DestBucket => match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => {
                    self.dest_info = Some(info);
                    let head = HeadObjectOperation::new(HeadObjectInput {
                        bucket: item.src_bucket,
                        key: item.src_key,
                        version_id: Some(item.src_ver),
                    });
                    self.step = CopyStep::Head;
                    Ok(Next::Call(IoCall::child(head)))
                }
                Err(err) => Ok(self.drop_intent(&intent, format!("dest bucket failed: {err}"))),
            },
            CopyStep::Head => {
                let head = match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => head,
                    Err(err) => {
                        return Ok(self.drop_intent(&intent, format!("source head failed: {err}")));
                    }
                };
                if let (Some(want), Some(loc)) = (&item.digest, &head.location) {
                    let actual = loc
                        .hashes
                        .get("BLAKE3")
                        .or_else(|| loc.hashes.get("blake3"));
                    if let Some(actual) = actual
                        && hex::encode(actual) != *want
                    {
                        return Ok(self.drop_intent(&intent, "source digest mismatch".into()));
                    }
                }
                let bytes = source_length(&head).unwrap_or(0);
                let run_str = self.firing_id.to_string();
                let date = format_date(self.now_ms);
                let dest_key =
                    match expand_copy(&self.dest_template, &date, &run_str, &item.src_key) {
                        Ok(key) => key,
                        Err(err) => return Ok(self.drop_intent(&intent, err)),
                    };
                let src_group = self.src_group.ok_or(IoError::Unfinished)?;
                let dest_group = self.dest_info.as_ref().ok_or(IoError::Unfinished)?.group_id;
                let src_path = object_permission_path(
                    self.local.1,
                    src_group,
                    self.local.0,
                    &item.src_bucket,
                    &item.src_key,
                );
                let dest_path = object_permission_path(
                    self.local.1,
                    dest_group,
                    self.local.0,
                    &self.dest_bucket,
                    &dest_key,
                );
                if !self
                    .spec
                    .scope
                    .write
                    .iter()
                    .any(|p| permission_pattern_matches(p, &dest_path))
                {
                    return Ok(self.drop_intent(
                        &intent,
                        format!("copy target `{dest_path}` outside scope.write"),
                    ));
                }
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                let call = authorize_path(auth, &src_path, Permission::READ);
                self.step = CopyStep::AuthSource(dest_key, bytes, dest_path);
                Ok(Next::Call(call))
            }
            CopyStep::AuthSource(dest_key, bytes, dest_path) => {
                if !allowed(result)? {
                    return Ok(self.drop_intent(&intent, "copy source read denied".into()));
                }
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                let call = authorize_path(auth, &dest_path, Permission::WRITE);
                self.step = CopyStep::AuthDest(dest_key, bytes);
                Ok(Next::Call(call))
            }
            CopyStep::AuthDest(dest_key, bytes) => {
                if !allowed(result)? {
                    return Ok(self.drop_intent(&intent, "copy target write denied".into()));
                }
                let cfg = GetConfigOperation::new(self.local.1);
                self.step = CopyStep::Config(dest_key, bytes);
                Ok(Next::Call(IoCall::child(cfg)))
            }
            CopyStep::Config(dest_key, bytes) => {
                let cfg = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(cfg) => cfg,
                    Err(err) => {
                        return Ok(
                            self.drop_intent(&intent, format!("realm config unavailable: {err}"))
                        );
                    }
                };
                let dest_group = self.dest_info.as_ref().ok_or(IoError::Unfinished)?.group_id;
                self.quota_ceiling = cfg.quota.effective_group_ceiling(&dest_group);
                let count = CountUsageOperation::copy_bytes(
                    self.auto.automation_id,
                    bytes,
                    &self.spec.scope.limits,
                );
                self.step = CopyStep::Count(dest_key, bytes);
                Ok(Next::Call(IoCall::retry(
                    count,
                    &self.auto.automation_id.to_bytes(),
                )))
            }
            CopyStep::Count(dest_key, bytes) => {
                if let Err(err) = result.child::<(), AutoError>()? {
                    return Ok(self.drop_intent(&intent, err.to_string()));
                }
                let dest_info = self.dest_info.as_ref().ok_or(IoError::Unfinished)?;
                let hint = dest_info.distribution.as_ref().map(|d| d.hint());
                let dest_ver = mint_version(self.ids.mint(self.now_ms), hint);
                let planned = CopiedFile {
                    source_key: item.src_key,
                    source_version: item.src_ver,
                    dest_bucket: self.dest_bucket.clone(),
                    dest_key,
                    dest_version: dest_ver,
                    bytes,
                };
                let encoded = postcard::to_allocvec(&planned)
                    .map_err(|err| AutoError::Storage(err.to_string()))?;
                let plan_row = (
                    AUTOMATION_RUNS_KEYSPACE.to_string(),
                    copy_plan_key(self.stage_id, item.ordinal),
                    ByteView::from(encoded),
                );
                self.step = CopyStep::WritePlan(Box::new(planned));
                Ok(Next::Call(IoCall::Write(vec![plan_row])))
            }
            CopyStep::WritePlan(planned) => {
                result.done()?;
                let auth = self.auth.clone().ok_or(IoError::Unfinished)?;
                let src_group = self.src_group.ok_or(IoError::Unfinished)?;
                let dest_group = self.dest_info.as_ref().ok_or(IoError::Unfinished)?.group_id;
                let allow_down = self.allow_downgrade && self.spec.scope.allow_downgrade;
                let dest_ver = planned.dest_version;
                let input = CopyObjectInput {
                    source_bucket: item.src_bucket,
                    source_key: item.src_key,
                    source_version_id: Some(item.src_ver),
                    source_group_id: src_group,
                    source_auth_context: auth.clone(),
                    dest_bucket: self.dest_bucket.clone(),
                    dest_key: planned.dest_key.clone(),
                    user_id: auth.user_id,
                    group_id: dest_group,
                    realm_id: self.local.1,
                    node_id: self.local.0,
                    quota_ceiling: self.quota_ceiling,
                    conditions: CopySourceConditions::default(),
                    metadata: None,
                    restrictions: auth.path_restrictions,
                    references: CopyReferences::Preserve,
                    job_id: Some(self.firing_id),
                };
                let op = CopyObjectOperation::new(input, NodeRouting::default())
                    .at_version(dest_ver)
                    .allow_downgrade(allow_down);
                self.step = CopyStep::Copy(planned);
                Ok(Next::Call(IoCall::child(op)))
            }
            CopyStep::Copy(planned) => match result.child::<CopyResultData, CopyObjectError>()? {
                Ok(res) => {
                    let copied = CopiedFile {
                        source_key: planned.source_key,
                        source_version: planned.source_version,
                        dest_bucket: planned.dest_bucket,
                        dest_key: planned.dest_key,
                        dest_version: res.version_id,
                        bytes: res.size.max(planned.bytes),
                    };
                    let reply = serde_json::to_string(&copied)
                        .map_err(|e| AutoError::Storage(e.to_string()))?;
                    let row =
                        settled_row_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, &intent, reply)
                            .map_err(|e| AutoError::Storage(e.detail()))?;
                    self.step = CopyStep::Settle(Box::new(copied));
                    Ok(Next::Call(IoCall::Write(vec![row])))
                }
                Err(
                    err @ (CopyObjectError::EncryptionDowngrade
                    | CopyObjectError::PreconditionFailed
                    | CopyObjectError::Put(
                        PutObjectError::QuotaExceeded { .. }
                        | PutObjectError::Distribution(_)
                        | PutObjectError::PolicyError(_)
                        | PutObjectError::PolicyGate(_),
                    )
                    | CopyObjectError::Get(
                        GetObjectError::DeleteMarker
                        | GetObjectError::NoSuchKey
                        | GetObjectError::NoSuchVersion,
                    )),
                ) => Ok(self.drop_intent(&intent, err.to_string())),
                Err(err) => {
                    let msg = err.to_string();
                    let row =
                        unknown_row_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, &intent, &msg)
                            .map_err(|e| AutoError::Storage(e.detail()))?;
                    self.step = CopyStep::MarkUnknown(msg, planned);
                    Ok(Next::Call(IoCall::Write(vec![row])))
                }
            },
            CopyStep::Settle(copied) => {
                result.done()?;
                let key = (
                    AUTOMATION_RUNS_KEYSPACE,
                    copy_plan_key(self.stage_id, item.ordinal).to_vec(),
                );
                self.step = CopyStep::CleanPlan(copied);
                Ok(Next::Call(delete_call(vec![key])))
            }
            CopyStep::CleanPlan(copied) => {
                result.done()?;
                Ok(Next::Done(StageOutcome::Progress(*copied)))
            }
            CopyStep::Drop(msg) => {
                result.done()?;
                Ok(Next::Done(StageOutcome::Failed(msg)))
            }
            CopyStep::MarkUnknown(msg, planned) => {
                result.done()?;
                Ok(Next::Done(StageOutcome::NeedsAttention(
                    msg,
                    Some(*planned),
                )))
            }
        }
    }
}
