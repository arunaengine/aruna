//! Runs plugin operations end to end for the tests, under the names the tests read best.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::compute::SecretBytes;
use aruna_core::plugin_installs::{PluginInstall, SecretBinding};
use aruna_core::plugin_runs::{HookRequest, PluginRun};
use aruna_core::plugins::Released;
use aruna_core::plugins::{Chunk, NodeCheckError, PublishedPlugin, ReleaseError, ReleaseRequest};
use aruna_core::secrets::{SecretKind, SecretName};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{HostError, JobStatus, ObjectPage};
use aruna_plugins::manifest::Manifest;
use serde_json::{Map, Value};
use ulid::Ulid;

use super::account::RepairAccount;
use super::artifacts::ArtifactError;
use super::bound::{BindError, BindPlace, BindSlots, BoundSlots, CheckNodeBinding};
use super::check::RunScope;
use super::copies::Assembled;
use super::copy_rows::{CopyTotal, KeepChunk, ReadChunk, ReadLocal, StoreCopy, SweepCopies};
use super::dispatch::{EnqueueTimers, HookAuthorization, StartError, StartRun, VerifyHook};
use super::held::AskRelease;
use super::hooks::{AdmitHook, HookError};
use super::install::{
    InstallCaller, InstallError, InstallPrepared, InstallRequest, PrepareInstall, Prepared,
};
use super::job_work::{CancelWork, JobState, RetireWork, SettleWork, StartWork};
use super::lifecycle::{RemoveInstall, SetGrants, SetPaused, UpdateInstall};
use super::publish::{CheckPublish, FinishPublish, PublishError, PublishStep};
use super::reconcile::{RepairSettled, repair_account};
use super::records::{InstallEntryConfig, InstallEntryOperation};
use super::rows::StoreError;
use super::rules::{EnsureRules, RetireRules};
use super::secret_use::{OpenSealed, ReleaseSecret, Sealed, SelectSecret, Selected};
use super::submit::WorkRequest;
use super::sweep::SweepOrphans;
use super::work::WorkRow;
use super::work_calls::{JobOutputs, JobRow};
use crate::driver::{DriverContext, drive};
pub(crate) use crate::plugin_host::read_config;
use crate::plugin_host::ready_artifact;

fn local_of(ctx: &DriverContext) -> Option<NodeId> {
    ctx.net_handle.as_ref().map(|net| net.node_id())
}

fn caller(actor: &Actor, auth: &AuthContext) -> InstallCaller {
    InstallCaller::new(actor, auth, unix_timestamp_millis())
}

pub(crate) async fn prepare_install(
    ctx: &DriverContext,
    auth: &AuthContext,
    realm_id: RealmId,
    request: &InstallRequest,
) -> Result<Prepared, InstallError> {
    let place = (realm_id, local_of(ctx));
    drive(PrepareInstall::new(auth, place, request), ctx).await
}

pub(crate) async fn install_plugin(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    request: &InstallRequest,
) -> Result<PluginInstall, InstallError> {
    let prepared = prepare_install(ctx, auth, actor.realm_id, request).await?;
    drive(InstallPrepared::new(caller(actor, auth), prepared), ctx).await
}

pub(crate) async fn write_record(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    expected: Option<u64>,
    install: PluginInstall,
    audit: Option<AdminAuditRecord>,
) -> Result<PluginInstall, InstallError> {
    let write = InstallEntryOperation::new(InstallEntryConfig {
        actor: actor.clone(),
        auth_context: auth.clone(),
        install_id: install.install_id,
        group_id: install.group_id,
        expected,
        install: Some(install.clone()),
        audit,
    });
    drive(write, ctx).await?;
    Ok(install)
}

pub(crate) async fn ensure_rules(
    ctx: &DriverContext,
    actor: &Actor,
    install: &PluginInstall,
    manifest: &Manifest,
) -> Result<(), String> {
    let ensure = EnsureRules::new(actor, install, manifest, unix_timestamp_millis())?;
    drive(ensure, ctx).await.map_err(|error| error.0)
}

pub(crate) async fn retire_rules(ctx: &DriverContext, actor: &Actor, install: &PluginInstall) {
    let retire = RetireRules::new(actor, install, unix_timestamp_millis());
    drive(retire, ctx).await.expect("the rules are retired");
}

fn repair_of(node: (NodeId, RealmId), install: &PluginInstall) -> RepairAccount {
    repair_account((node.1, node.0), install, unix_timestamp_millis())
}

pub(crate) async fn repair_install(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    install: &PluginInstall,
) -> Result<bool, InstallError> {
    Ok(drive(repair_of((node_id, realm_id), install), ctx).await?)
}

pub(crate) async fn repair_settled(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    installs: &[PluginInstall],
) -> usize {
    let repair = RepairSettled::new((realm_id, node_id), installs, unix_timestamp_millis());
    drive(repair, ctx).await.expect("the repair finishes")
}

pub(crate) async fn sweep_orphans(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, String> {
    let sweep = SweepOrphans::new(Arc::new(config.clone()), node_id, now_ms);
    drive(sweep, ctx).await.map_err(|error| error.0)
}

pub(crate) async fn set_grants(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    choices: &BTreeMap<String, Vec<String>>,
) -> Result<PluginInstall, InstallError> {
    drive(
        SetGrants::new(caller(actor, auth), install_id, choices),
        ctx,
    )
    .await
}

pub(crate) async fn update_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
    digest: &str,
    settings: Option<&Map<String, Value>>,
) -> Result<(PluginInstall, Vec<String>), InstallError> {
    let who = (caller(actor, auth), local_of(ctx));
    drive(UpdateInstall::new(who, install_id, digest, settings), ctx).await
}

pub(crate) async fn pause_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    drive(SetPaused::new(caller(actor, auth), install_id, true), ctx).await
}

pub(crate) async fn resume_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    drive(SetPaused::new(caller(actor, auth), install_id, false), ctx).await
}

pub(crate) async fn remove_install(
    ctx: &DriverContext,
    actor: &Actor,
    auth: &AuthContext,
    install_id: Ulid,
) -> Result<PluginInstall, InstallError> {
    drive(RemoveInstall::new(caller(actor, auth), install_id), ctx).await
}

pub(crate) async fn start_run(
    ctx: &DriverContext,
    auth: &AuthContext,
    realm_id: RealmId,
    local: NodeId,
    install_id: Ulid,
    input: &str,
    now_ms: u64,
) -> Result<PluginRun, StartError> {
    let start = StartRun::new(auth, (realm_id, local), install_id, input, now_ms);
    drive(start, ctx).await
}

pub(crate) async fn enqueue_timers(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    now_ms: u64,
) -> Result<usize, StoreError> {
    drive(EnqueueTimers::new(config, local, now_ms), ctx).await
}

pub(crate) async fn admit_hook(
    ctx: &DriverContext,
    local: NodeId,
    (realm_id, install_id): (RealmId, Ulid),
    request: HookRequest,
    now_ms: u64,
) -> Result<bool, HookError> {
    let verify = VerifyHook::new((realm_id, local), install_id, request, now_ms);
    let hook = drive(verify, ctx).await?;
    let admit = AdmitHook::new(hook.run, (hook.install_id, hook.delivery_id), now_ms)?;
    Ok(drive(admit, ctx).await?)
}

pub(crate) async fn checks_authorization(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    place: (NodeId, Ulid),
) -> Option<bool> {
    let check = HookAuthorization::new(config.clone(), place);
    drive(check, ctx).await.ok().flatten()
}

pub(crate) async fn select(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    install: &PluginInstall,
    binding: &SecretBinding,
) -> Result<Option<Selected>, ReleaseError> {
    let select = SelectSecret::new(Arc::new(config.clone()), install, binding);
    drive(select, ctx).await.map_err(|failure| failure.0)
}

pub(crate) async fn open(
    ctx: &DriverContext,
    realm_id: RealmId,
    sealed: &Sealed,
) -> Result<SecretBytes, ReleaseError> {
    let open = OpenSealed::new(realm_id, sealed.clone());
    drive(open, ctx).await.map_err(|failure| failure.0)
}

pub(crate) async fn bind_slots(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    place: BindPlace,
    manifest: &Manifest,
    requested: &BTreeMap<String, String>,
) -> Result<BoundSlots, BindError> {
    let bind = BindSlots::new(Arc::new(config.clone()), place, manifest, requested);
    drive(bind, ctx).await
}

pub(crate) async fn check_node_binding(
    ctx: &DriverContext,
    place: (NodeId, NodeId),
    name: &SecretName,
    kind: &SecretKind,
) -> Result<Option<String>, NodeCheckError> {
    let check = CheckNodeBinding::new(place, name, kind);
    drive(check, ctx).await.map_err(|failure| failure.0)
}

pub(crate) async fn ask_release(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    request: ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let ask = AskRelease::new(Arc::new(config.clone()), local, request);
    drive(ask, ctx).await.map_err(|failure| failure.0)
}

pub(crate) async fn release(
    ctx: &DriverContext,
    (config, local, peer): (&RealmConfigDocument, NodeId, NodeId),
    request: &ReleaseRequest,
) -> Result<Released, ReleaseError> {
    let nodes = (local, peer);
    let release = ReleaseSecret::new(Arc::new(config.clone()), nodes, request.clone());
    drive(release, ctx).await.map_err(|failure| failure.0)
}

pub(crate) async fn has_local(ctx: &DriverContext, digest: &str) -> Result<bool, ArtifactError> {
    Ok(drive(CopyTotal::new(digest), ctx).await?.is_some())
}

pub(crate) async fn read_local(
    ctx: &DriverContext,
    digest: &str,
) -> Result<Option<Assembled>, ArtifactError> {
    drive(ReadLocal::new(digest), ctx).await
}

pub(crate) async fn read_chunk(
    ctx: &DriverContext,
    digest: &str,
    index: u32,
) -> Result<Option<(u64, Vec<u8>)>, ArtifactError> {
    drive(ReadChunk::new(digest, index), ctx).await
}

pub(crate) async fn store_local(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
    bytes: &[u8],
    now_ms: u64,
) -> Result<(), ArtifactError> {
    drive(StoreCopy::new(config, digest, bytes, now_ms), ctx).await
}

pub(crate) async fn store_chunk(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    chunk: Chunk,
    now_ms: u64,
) -> Result<bool, ArtifactError> {
    drive(KeepChunk::new(config, chunk, now_ms), ctx).await
}

pub(crate) async fn sweep_copies(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, ArtifactError> {
    drive(SweepCopies::new(config, now_ms), ctx).await
}

pub(crate) async fn fetch_artifact(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    digest: &str,
) -> Result<Assembled, ArtifactError> {
    ready_artifact(ctx, config, digest, local_of(ctx)).await
}

pub(crate) async fn publish_plugin(
    ctx: &DriverContext,
    actor: Actor,
    auth: AuthContext,
    artifact: Vec<u8>,
) -> Result<PublishedPlugin, PublishError> {
    let check = CheckPublish::new(&actor, &auth, &artifact, unix_timestamp_millis())?;
    let plan = match drive(check, ctx).await? {
        PublishStep::Known(published) => return Ok(published),
        PublishStep::Store(plan) => plan,
    };
    drive(plan.store_copy(&artifact), ctx).await?;
    drive(FinishPublish::new(actor, auth, *plan, artifact), ctx).await
}

pub(crate) async fn job_row(
    ctx: &DriverContext,
    install: &PluginInstall,
    id: &str,
) -> Result<WorkRow, HostError> {
    Ok(drive(JobRow::new(install, id), ctx).await?)
}

pub(crate) async fn job_status(
    ctx: &DriverContext,
    place: (&PluginInstall, NodeId),
    id: &str,
) -> Result<JobStatus, HostError> {
    Ok(drive(JobState::new(place, id), ctx).await?)
}

pub(crate) async fn cancel_job(
    ctx: &DriverContext,
    install: &PluginInstall,
    id: &str,
    now_ms: u64,
) -> Result<(), HostError> {
    Ok(drive(CancelWork::new(install, id, now_ms), ctx).await?)
}

pub(crate) async fn start_work(
    ctx: &DriverContext,
    scope: &RunScope,
    request: WorkRequest,
) -> Result<Ulid, HostError> {
    let minted = (unix_timestamp_millis(), Ulid::generate());
    let start = StartWork::new(scope, request, minted, ctx.node_and_realm());
    Ok(drive(start, ctx).await?)
}

pub(crate) async fn settle_work(ctx: &DriverContext, now_ms: u64) -> Result<usize, StoreError> {
    drive(SettleWork::new(now_ms), ctx).await
}

pub(crate) async fn retire_work(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    now_ms: u64,
) -> Result<usize, StoreError> {
    drive(RetireWork::new(config, now_ms), ctx).await
}

pub(crate) async fn job_outputs(
    ctx: &DriverContext,
    place: (&PluginInstall, NodeId),
    id: &str,
    cursor: Option<String>,
) -> Result<ObjectPage, HostError> {
    Ok(drive(JobOutputs::new(place, id, cursor), ctx).await?)
}
