//! Changes one applet on its host node: lifecycle steps, approvals, embedding, guests, domains
//! and new versions, each checked against the caller's rights and the record's revision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::hosts::{HostKind, HostRow, host_key};
use aruna_core::applet::notices::{Notice, notice_key};
use aruna_core::applet::record::{
    Access, Applet, ApprovalRequest, CustomDomain, DomainState, Draft, Grants, GuestPolicy,
    Lifecycle, PinnedObject, VersionRef, VersionState,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{APPLET_HOSTS_KEYSPACE, APPLETS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{Key, KeySpace, RoleId, Value};
use aruna_core::user::id::UserId;
use ulid::Ulid;

use super::identity::SyncIdentityOperation;
use super::{
    AppletAccessOperation, AppletError, Need, applet_answer, host_row, read_applet, record_rows,
};

/// One change of an applet record.
#[derive(Clone, Debug, PartialEq)]
pub enum AppletChange {
    RequestApproval {
        version: u32,
        access: Access,
        grants: Grants,
        origins: Vec<String>,
    },
    Approve {
        version: u32,
    },
    Reject,
    Rollback {
        version: u32,
    },
    Pause,
    Resume,
    TakeDown {
        version: Option<u32>,
    },
    /// Switches embedding and keeps only already approved origins; adding one needs approval.
    Embedding {
        enabled: bool,
        origins: Vec<String>,
    },
    Guests(Option<GuestPolicy>),
    AddDomain {
        host: String,
        nonce: String,
    },
    RemoveDomain {
        host: String,
    },
    /// A domain check of this node passed or failed.
    CheckDomain {
        host: String,
        passed: bool,
    },
    AddVersion(Box<VersionRef>),
    /// A builder job started now; refused when the applet keeps `max` drafts already. The
    /// draft lives `ttl_ms` from now.
    StartBuild {
        draft: Box<Draft>,
        max: usize,
        ttl_ms: u64,
    },
    /// A build's outputs were captured into the draft manifest `manifest`.
    CaptureDraft {
        build_id: Ulid,
        manifest: PinnedObject,
    },
    FailDraft {
        build_id: Ulid,
        reason: String,
    },
    DropDraft {
        build_id: Ulid,
    },
    /// Turns a captured draft into the private version `entry` with the same pinned files.
    KeepDraft {
        build_id: Ulid,
        entry: Box<VersionRef>,
    },
    /// Expires drafts and drops versions beyond the kept ones.
    Prune,
    /// The applet's account and role as the node made them.
    Identity {
        account: Option<UserId>,
        role_id: Option<RoleId>,
    },
    /// Replaces the list of group automations triggered by submissions to this applet.
    Automations(Vec<Ulid>),
}

impl AppletChange {
    /// What the caller needs on the applet for this change.
    pub fn need(&self) -> Need {
        match self {
            Self::Approve { .. } => Need::Publish,
            _ => Need::Write,
        }
    }

    /// Whether the change can alter what an edge decided for the applet's hosts.
    pub fn notifies(&self) -> bool {
        matches!(
            self,
            Self::Approve { .. }
                | Self::Rollback { .. }
                | Self::Pause
                | Self::Resume
                | Self::TakeDown { .. }
                | Self::Embedding { .. }
                | Self::Guests(_)
                | Self::RemoveDomain { .. }
                | Self::AddVersion(_)
                | Self::KeepDraft { .. }
        )
    }

    /// Whether the change can turn the applet's account and role on or off.
    pub fn moves_identity(&self) -> bool {
        matches!(
            self,
            Self::Approve { .. } | Self::Pause | Self::Resume | Self::TakeDown { .. }
        )
    }
}

/// How a change moves the host index: rows to add and hosts to remove.
#[derive(Debug, Default, PartialEq)]
pub struct HostMoves {
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

/// Applies `change` to `applet` as `actor` at `now_ms`. Answers the host rows it moves.
pub fn apply(
    applet: &mut Applet,
    change: AppletChange,
    actor: UserId,
    now_ms: u64,
) -> Result<HostMoves, AppletError> {
    let invalid = |error: aruna_core::applet::record::TransitionError| {
        AppletError::Conflict(error.to_string())
    };
    let mut moves = HostMoves::default();
    match change {
        AppletChange::RequestApproval {
            version,
            access,
            grants,
            origins,
        } => {
            let request = ApprovalRequest {
                version,
                access,
                grants,
                origins,
                requested_by: actor,
                requested_at_ms: now_ms,
            };
            applet.request_approval(request).map_err(invalid)?;
        }
        AppletChange::Approve { version } => applet.approve(version, actor).map_err(invalid)?,
        AppletChange::Reject => applet.reject().map_err(invalid)?,
        AppletChange::Rollback { version } => applet.rollback(version).map_err(invalid)?,
        AppletChange::Pause => applet.pause().map_err(invalid)?,
        AppletChange::Resume => applet.resume().map_err(invalid)?,
        AppletChange::TakeDown { version } => applet.take_down(version).map_err(invalid)?,
        AppletChange::Embedding { enabled, origins } => {
            if let Some(added) =
                (origins.iter()).find(|origin| !applet.embedding.origins.contains(origin))
            {
                return Err(AppletError::Invalid(format!(
                    "adding the origin {added} needs approval"
                )));
            }
            applet.embedding.enabled = enabled;
            applet.embedding.origins = origins;
        }
        AppletChange::Guests(policy) => applet.guests = policy,
        AppletChange::AddDomain { host, nonce } => {
            if applet.domains.iter().any(|domain| domain.host == host) {
                return Err(AppletError::Conflict(format!("{host} is already added")));
            }
            applet.domains.push(CustomDomain {
                host,
                nonce,
                state: DomainState::Pending,
                failures: 0,
                checked_at_ms: now_ms,
            });
        }
        AppletChange::RemoveDomain { host } => {
            let before = applet.domains.len();
            applet.domains.retain(|domain| domain.host != host);
            if applet.domains.len() == before {
                return Err(AppletError::NotFound);
            }
            moves.remove.push(host);
        }
        AppletChange::CheckDomain { host, passed } => {
            let domain = (applet.domains.iter_mut())
                .find(|domain| domain.host == host)
                .ok_or(AppletError::NotFound)?;
            let was = domain.state;
            domain.record_check(passed, now_ms);
            match (
                was == DomainState::Active,
                domain.state == DomainState::Active,
            ) {
                (false, true) => moves.add.push(host),
                (true, false) => moves.remove.push(host),
                _ => {}
            }
        }
        AppletChange::StartBuild {
            mut draft,
            max,
            ttl_ms,
        } => {
            if applet.lifecycle == Lifecycle::TakenDown {
                return Err(AppletError::Conflict("the applet is taken down".into()));
            }
            if applet.draft_room(max) == 0 {
                return Err(AppletError::Conflict(format!(
                    "the applet keeps at most {max} drafts; keep or delete one first"
                )));
            }
            draft.started_at_ms = now_ms;
            draft.expires_at_ms = now_ms.saturating_add(ttl_ms);
            applet.drafts.push(*draft);
        }
        AppletChange::CaptureDraft { build_id, manifest } => {
            let draft = open_draft(applet, build_id)?;
            draft.manifest = Some(manifest);
        }
        AppletChange::FailDraft { build_id, reason } => {
            open_draft(applet, build_id)?.failed = Some(reason);
        }
        AppletChange::DropDraft { build_id } => {
            applet.draft(build_id).ok_or(AppletError::NotFound)?;
            applet.drafts.retain(|draft| draft.build_id != build_id);
        }
        AppletChange::KeepDraft { build_id, entry } => {
            let draft = applet.draft(build_id).ok_or(AppletError::NotFound)?;
            if draft.manifest.is_none() {
                return Err(AppletError::Conflict(
                    "the build has no captured outputs".into(),
                ));
            }
            add_version(applet, *entry)?;
            applet.drafts.retain(|draft| draft.build_id != build_id);
        }
        AppletChange::AddVersion(entry) => add_version(applet, *entry)?,
        AppletChange::Prune => {
            applet.retain(now_ms);
        }
        AppletChange::Identity { account, role_id } => {
            applet.account = account;
            applet.role_id = role_id;
        }
        AppletChange::Automations(mut ids) => {
            if applet.lifecycle == Lifecycle::TakenDown {
                return Err(AppletError::Conflict("the applet is taken down".into()));
            }
            let mut seen = BTreeSet::new();
            ids.retain(|id| seen.insert(*id));
            applet.automations = ids;
        }
    }
    applet.revision += 1;
    Ok(moves)
}

/// The notice that makes edges drop what they cached for every host of `applet`.
fn notice_row(
    applet: &Applet,
    moves: &HostMoves,
    now_ms: u64,
) -> Result<(KeySpace, Key, Value), AppletError> {
    let hosts = std::iter::once(&applet.host)
        .chain(applet.domains.iter().map(|domain| &domain.host))
        .chain(&moves.remove)
        .cloned()
        .collect();
    let notice = Notice {
        applet_id: applet.applet_id,
        hosts,
    };
    Ok((
        APPLETS_KEYSPACE.to_string(),
        notice_key(now_ms, applet.applet_id, applet.revision),
        Value::from(notice.to_bytes()?),
    ))
}

/// A draft whose build has neither outputs nor a failure yet.
fn open_draft(applet: &mut Applet, build_id: Ulid) -> Result<&mut Draft, AppletError> {
    let draft = (applet.drafts.iter_mut())
        .find(|draft| draft.build_id == build_id)
        .ok_or(AppletError::NotFound)?;
    if draft.manifest.is_some() || draft.failed.is_some() {
        return Err(AppletError::Conflict("the build already finished".into()));
    }
    Ok(draft)
}

fn add_version(applet: &mut Applet, entry: VersionRef) -> Result<(), AppletError> {
    if applet.lifecycle == Lifecycle::TakenDown {
        return Err(AppletError::Conflict("the applet is taken down".into()));
    }
    if entry.version != applet.next_version() || entry.state != VersionState::Private {
        return Err(AppletError::Conflict("the version number is stale".into()));
    }
    applet.versions.push(entry);
    Ok(())
}

#[derive(Debug, PartialEq)]
enum ChangeStep {
    Access,
    Write,
    Identity,
}

/// Checks the caller's rights, then applies the change in one write transaction on the host
/// node. A stale `expected` revision is a conflict, so two editors never overwrite each other.
#[derive(Debug, PartialEq)]
pub struct ChangeAppletOperation {
    auth: AuthContext,
    applet_id: Ulid,
    change: Option<AppletChange>,
    expected: Option<u64>,
    local: NodeId,
    /// The account and role follow the record after the write.
    identity: bool,
    step: ChangeStep,
}

impl ChangeAppletOperation {
    pub fn new(
        auth: &AuthContext,
        (applet_id, expected): (Ulid, Option<u64>),
        change: AppletChange,
        local: NodeId,
    ) -> Self {
        Self {
            auth: auth.clone(),
            applet_id,
            identity: change.moves_identity(),
            change: Some(change),
            expected,
            local,
            step: ChangeStep::Access,
        }
    }
}

impl Operation for ChangeAppletOperation {
    type Output = Applet;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        let need = self.change.as_ref().map_or(Need::Write, AppletChange::need);
        let access = AppletAccessOperation::new(&self.auth, self.applet_id, need);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        match std::mem::replace(&mut self.step, ChangeStep::Write) {
            ChangeStep::Access => {
                let applet = result.child::<Applet, AppletError>()??;
                if applet.host_node != self.local {
                    return Err(AppletError::Elsewhere(applet.host_node));
                }
                let write = WriteChangeOperation {
                    applet_id: self.applet_id,
                    expected: self.expected,
                    change: self.change.take().ok_or(AppletError::NotFound)?,
                    actor: self.auth.user_id,
                    now_ms: 0,
                    record: None,
                    deletes: Vec::new(),
                    writes: Vec::new(),
                    checking: false,
                };
                let seed = self.applet_id.to_bytes();
                Ok(Next::Call(IoCall::retry(write, &seed)))
            }
            ChangeStep::Write => {
                let applet = result.child::<Applet, AppletError>()??;
                let needed = applet.account.is_some() || !applet.grants.is_empty();
                if !(self.identity && needed) {
                    return Ok(Next::Done(applet));
                }
                let sync = SyncIdentityOperation::new(&applet, (self.local, self.auth.realm_id));
                self.step = ChangeStep::Identity;
                Ok(Next::Call(IoCall::child(sync)))
            }
            ChangeStep::Identity => result.child::<Applet, AppletError>()?.map(Next::Done),
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

/// Reads the record, applies one change and writes it with its host rows in one transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteChangeOperation {
    applet_id: Ulid,
    expected: Option<u64>,
    change: AppletChange,
    actor: UserId,
    now_ms: u64,
    record: Option<Applet>,
    deletes: Vec<String>,
    writes: Vec<(KeySpace, Key, Value)>,
    checking: bool,
}

impl WriteChangeOperation {
    /// A change the node makes itself, such as a domain check or a captured version.
    pub fn node(applet_id: Ulid, change: AppletChange, actor: UserId) -> Self {
        Self {
            applet_id,
            expected: None,
            change,
            actor,
            now_ms: 0,
            record: None,
            deletes: Vec::new(),
            writes: Vec::new(),
            checking: false,
        }
    }
}

impl Operation for WriteChangeOperation {
    type Output = Applet;
    type Error = AppletError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        Ok(Next::Call(read_applet(self.applet_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        if self.checking {
            self.checking = false;
            let values = result.values()?;
            let candidates = std::mem::take(&mut self.deletes);
            for (host, row) in candidates.into_iter().zip(values) {
                let owned =
                    super::host_answer(row)?.is_some_and(|row| row.applet_id == self.applet_id);
                if owned {
                    self.deletes.push(host);
                }
            }
            let writes = std::mem::take(&mut self.writes);
            return Ok(Next::Call(IoCall::Write(writes)));
        }
        if let Some(record) = self.record.take() {
            result.done()?;
            if self.deletes.is_empty() {
                return Ok(Next::Done(record));
            }
            // Only host rows owned by this applet are deleted, in the same transaction.
            let deletes = (std::mem::take(&mut self.deletes).iter())
                .map(|host| (APPLET_HOSTS_KEYSPACE.to_string(), host_key(host)))
                .collect();
            self.record = Some(record);
            return Ok(Next::Call(IoCall::Delete(deletes)));
        }
        let mut applet = applet_answer(result)?.ok_or(AppletError::NotFound)?;
        if self
            .expected
            .is_some_and(|expected| expected != applet.revision)
        {
            return Err(AppletError::Conflict(
                "the applet changed meanwhile; read it again".into(),
            ));
        }
        let notifies = self.change.notifies();
        let moves = apply(&mut applet, self.change.clone(), self.actor, self.now_ms)?;
        let mut writes = record_rows(&applet)?;
        if notifies || !moves.add.is_empty() || !moves.remove.is_empty() {
            writes.push(notice_row(&applet, &moves, self.now_ms)?);
        }
        for host in &moves.add {
            let row = HostRow {
                applet_id: applet.applet_id,
                kind: HostKind::Custom,
            };
            writes.push(host_row(host, row)?);
        }
        self.record = Some(applet);
        self.deletes = moves.remove;
        if !self.deletes.is_empty() {
            let reads = self
                .deletes
                .iter()
                .map(|host| (APPLET_HOSTS_KEYSPACE.to_string(), host_key(host)))
                .collect();
            self.writes = writes;
            self.checking = true;
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

#[cfg(test)]
#[path = "change_tests.rs"]
pub(crate) mod tests;
