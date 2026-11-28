//! Creates a private applet on this node and claims its host in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::hosts::{HostKind, HostRow, applet_host, host_key};
use aruna_core::applet::record::{Access, Applet, Embedding, Grants, Lifecycle};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::APPLET_HOSTS_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::{AppletError, allowed, authorize_path, host_answer, host_row, record_rows};

/// Longest applet name, in characters.
pub const MAX_NAME: usize = 120;

/// What a caller asks for when it creates an applet.
#[derive(Clone, Debug, PartialEq)]
pub struct NewApplet {
    pub group_id: GroupId,
    pub name: String,
    pub slug: String,
    pub repository: Option<Ulid>,
}

/// This node's hosting settings a creation needs, and the salt the transport drew.
#[derive(Clone, Debug, PartialEq)]
pub struct Hosting {
    pub node: NodeId,
    pub applet_domain: String,
    pub visitor_salt: [u8; 32],
}

#[derive(Debug, PartialEq)]
enum CreateStep {
    Authorize,
    Write,
}

/// Checks WRITE on the group's data here, then stores the private applet with its host row.
#[derive(Debug, PartialEq)]
pub struct CreateAppletOperation {
    auth: AuthContext,
    request: NewApplet,
    hosting: Hosting,
    ids: IdMint,
    now_ms: u64,
    step: CreateStep,
}

impl CreateAppletOperation {
    pub fn new(auth: &AuthContext, request: NewApplet, hosting: Hosting) -> Self {
        Self {
            auth: auth.clone(),
            request,
            hosting,
            ids: IdMint::default(),
            now_ms: 0,
            step: CreateStep::Authorize,
        }
    }

    fn record(&mut self) -> Result<Applet, AppletError> {
        let name = self.request.name.trim();
        if name.is_empty() || name.chars().count() > MAX_NAME {
            return Err(AppletError::Invalid(format!(
                "names need 1 to {MAX_NAME} characters"
            )));
        }
        let host = applet_host(&self.request.slug, &self.hosting.applet_domain)
            .map_err(|error| AppletError::Invalid(error.to_string()))?;
        Ok(Applet {
            applet_id: self.ids.mint(self.now_ms),
            group_id: self.request.group_id,
            name: name.to_string(),
            slug: self.request.slug.clone(),
            host,
            repository: self.request.repository,
            host_node: self.hosting.node,
            lifecycle: Lifecycle::Private,
            access: Access::Private,
            live: None,
            versions: Vec::new(),
            drafts: Vec::new(),
            grants: Grants::default(),
            embedding: Embedding::default(),
            guests: None,
            domains: Vec::new(),
            pending: None,
            account: None,
            role_id: None,
            visitor_salt: self.hosting.visitor_salt,
            created_by: self.auth.user_id,
            created_at_ms: self.now_ms,
            revision: 1,
            automations: Vec::new(),
        })
    }
}

impl Operation for CreateAppletOperation {
    type Output = Applet;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        let realm = self.auth.realm_id;
        let path = group_permission_path(realm, self.request.group_id, self.hosting.node);
        Ok(Next::Call(authorize_path(
            &self.auth,
            &path,
            Permission::WRITE,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        match std::mem::replace(&mut self.step, CreateStep::Write) {
            CreateStep::Authorize => {
                if !allowed(result)? {
                    return Err(AppletError::Forbidden);
                }
                let record = self.record()?;
                let seed = record.applet_id.to_bytes();
                let write = StoreAppletOperation {
                    record,
                    read: false,
                };
                Ok(Next::Call(IoCall::retry(write, &seed)))
            }
            CreateStep::Write => result.child::<Applet, AppletError>()?.map(Next::Done),
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(
            error,
            AppletError::Forbidden | AppletError::Invalid(_) | AppletError::Conflict(_)
        )
    }
}

/// Writes a new record, its group index row and its host row, unless the host is taken.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreAppletOperation {
    record: Applet,
    read: bool,
}

impl Operation for StoreAppletOperation {
    type Output = Applet;
    type Error = AppletError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        let row = host_key(&self.record.host);
        Ok(Next::Call(IoCall::Read(vec![(
            APPLET_HOSTS_KEYSPACE.to_string(),
            row,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        if std::mem::replace(&mut self.read, true) {
            result.done()?;
            return Ok(Next::Done(self.record.clone()));
        }
        if host_answer(result.value()?)?.is_some() {
            return Err(AppletError::Conflict(format!(
                "the host {} is taken",
                self.record.host
            )));
        }
        let mut writes = record_rows(&self.record)?;
        let row = HostRow {
            applet_id: self.record.applet_id,
            kind: HostKind::Applet,
        };
        writes.push(host_row(&self.record.host, row)?);
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(error, AppletError::Conflict(_))
    }
}
