//! Changes one applet on its host node: lifecycle steps, approvals, embedding, guests, domains
//! and new versions, each checked against the caller's rights and the record's revision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::hosts::{HostKind, HostRow, host_key};
use aruna_core::applet::record::{
    Access, Applet, ApprovalRequest, CustomDomain, DomainState, Grants, GuestPolicy, VersionRef,
    VersionState,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::APPLET_HOSTS_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::user::id::UserId;
use ulid::Ulid;

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
}

impl AppletChange {
    /// What the caller needs on the applet for this change.
    pub fn need(&self) -> Need {
        match self {
            Self::Approve { .. } => Need::Publish,
            _ => Need::Write,
        }
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
        AppletChange::AddVersion(entry) => {
            if applet.lifecycle == aruna_core::applet::record::Lifecycle::TakenDown {
                return Err(AppletError::Conflict("the applet is taken down".into()));
            }
            if entry.version != applet.next_version() || entry.state != VersionState::Private {
                return Err(AppletError::Conflict("the version number is stale".into()));
            }
            applet.versions.push(*entry);
        }
    }
    applet.revision += 1;
    Ok(moves)
}

#[derive(Debug, PartialEq)]
enum ChangeStep {
    Access,
    Write,
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
                };
                let seed = self.applet_id.to_bytes();
                Ok(Next::Call(IoCall::retry(write, &seed)))
            }
            ChangeStep::Write => result.child::<Applet, AppletError>()?.map(Next::Done),
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
        if let Some(record) = self.record.take() {
            result.done()?;
            if self.deletes.is_empty() {
                return Ok(Next::Done(record));
            }
            // The removed hosts go in the same transaction as the record write.
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
        let moves = apply(&mut applet, self.change.clone(), self.actor, self.now_ms)?;
        let mut writes = record_rows(&applet)?;
        for host in &moves.add {
            let row = HostRow {
                applet_id: applet.applet_id,
                kind: HostKind::Custom,
            };
            writes.push(host_row(host, row)?);
        }
        self.record = Some(applet);
        self.deletes = moves.remove;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}
