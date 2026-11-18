//! Prepares native work for a run on the receiver: the per call checks and the work row naming
//! it across calls. The job is submitted afterwards, only while its dispatch is unresolved.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::{PluginWorkAction, PluginWorkSpec, UploadForm};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::check::{Authorize, CallError, CheckCall, CopyGrants, RunScope, matching};
use super::grants::InstallScope;
use super::objects::{LocalObject, Unmarked};
use super::rows::encode;
use super::unavailable;
use super::work::{
    Identity, RequestContext, Reserve, ReserveWork, WorkError, WorkKind, WorkRow, WorkState,
    reserve,
};

/// Unfinished in-process work one install may have at a time.
pub const WORK_ACTIVE: u32 = 8;

/// A copy a run asks for: object paths below the group's data on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyRequest {
    pub source: String,
    pub source_version: Option<Ulid>,
    pub target: String,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// A download a run asks for: the request resolved when the call came, into an object path below
/// the group's data on this node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadRequest {
    pub context: RequestContext,
    pub target: String,
    pub checksums: Vec<(ChecksumAlgorithm, Vec<u8>)>,
    pub max_bytes: Option<u64>,
    /// Whether the connection injects a secret; the version the download writes is marked then.
    pub secret: bool,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// An upload a run asks for: the request resolved when the call came, and the object it sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadRequest {
    pub context: RequestContext,
    pub source: String,
    pub version: Option<Ulid>,
    pub form: Option<UploadForm>,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

/// The native work a run asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkRequest {
    Copy(CopyRequest),
    Download(DownloadRequest),
    Upload(UploadRequest),
}

/// How a work request stands after its checks and its row.
#[derive(Debug, PartialEq)]
pub(crate) enum PreparedWork {
    /// The row names a job already; the call answers its work id.
    Known(Ulid),
    /// The row is reserved; this job is to be submitted.
    Submit {
        account: UserId,
        spec: Box<PluginWorkSpec>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Check,
    CopyGrants,
    Connection,
    DownloadGrant,
    UploadGrant,
    Paused,
    Source,
    SourceVersion,
    Target,
    Reserve,
}

/// Checks a work request step by step and reserves its row. It needs the grants of the request's
/// kind; a paused install starts nothing, and every attempt checks the version it reads again.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareWork {
    scope: RunScope,
    request: WorkRequest,
    now_ms: u64,
    /// The version id a new row of a writing work takes.
    minted: Ulid,
    steps: VecDeque<Step>,
    current: Step,
    install: Option<PluginInstall>,
    place: Option<(String, String)>,
}

impl PrepareWork {
    pub(crate) fn new(
        scope: &RunScope,
        request: WorkRequest,
        (now_ms, minted): (u64, Ulid),
    ) -> Self {
        use Step::*;
        let steps: &[Step] = match request {
            WorkRequest::Copy(_) => &[
                CopyGrants,
                Paused,
                Source,
                SourceVersion,
                Target,
                Step::Reserve,
            ],
            WorkRequest::Download(_) => &[Connection, DownloadGrant, Paused, Target, Step::Reserve],
            WorkRequest::Upload(_) => &[
                Connection,
                UploadGrant,
                Paused,
                Source,
                SourceVersion,
                Step::Reserve,
            ],
        };
        Self {
            scope: scope.clone(),
            request,
            now_ms,
            minted,
            steps: steps.iter().copied().collect(),
            current: Check,
            install: None,
            place: None,
        }
    }

    fn install(&self) -> Result<&PluginInstall, CallError> {
        Ok(self.install.as_ref().ok_or(IoError::Unfinished)?)
    }

    fn source(&self) -> &str {
        match &self.request {
            WorkRequest::Copy(copy) => &copy.source,
            WorkRequest::Upload(upload) => &upload.source,
            WorkRequest::Download(download) => &download.target,
        }
    }

    fn target(&self) -> &str {
        match &self.request {
            WorkRequest::Copy(copy) => &copy.target,
            WorkRequest::Download(download) => &download.target,
            WorkRequest::Upload(upload) => &upload.source,
        }
    }

    fn connection(&self) -> Result<String, CallError> {
        let context = match &self.request {
            WorkRequest::Download(download) => &download.context,
            WorkRequest::Upload(upload) => &upload.context,
            WorkRequest::Copy(_) => return Err(IoError::Unfinished.into()),
        };
        let install = self.install()?;
        let scope = InstallScope {
            realm_id: self.scope.realm_id,
            group_id: install.group_id,
            install_id: install.install_id,
        };
        Ok(scope.connection(&context.connection))
    }

    fn locate(&self, path: &str) -> Result<IoCall, CallError> {
        let place = (self.scope.realm_id, self.scope.local);
        let locate = LocalObject::new(place, self.install()?, path);
        Ok(IoCall::child(locate))
    }

    fn grant(&self, permission: Permission, path: &str) -> Result<IoCall, CallError> {
        let install = self.install()?;
        let patterns = matching(install, permission, path)?;
        Ok(IoCall::child(Authorize::call(
            install, permission, path, &patterns,
        )))
    }

    fn call(&self, step: Step) -> Result<IoCall, CallError> {
        match step {
            Step::Check => Ok(IoCall::child(CheckCall::new(&self.scope, None))),
            Step::CopyGrants => {
                let paths = (self.source(), self.target());
                Ok(IoCall::child(CopyGrants::new(self.install()?, paths)))
            }
            Step::Connection => self.grant(Permission::ConnectionsSend, &self.connection()?),
            Step::DownloadGrant => self.grant(Permission::TransfersDownload, self.target()),
            Step::UploadGrant => {
                let install = self.install()?;
                let upload = Permission::TransfersUpload;
                let patterns = matching(install, upload, self.source())?;
                let read = (upload, Access::READ);
                let check = Authorize::access(install, read, self.source(), &patterns);
                Ok(IoCall::child(check))
            }
            Step::Source => self.locate(self.source()),
            Step::Target => self.locate(self.target()),
            Step::SourceVersion => {
                let (bucket, key) = self.place.as_ref().ok_or(IoError::Unfinished)?;
                let version = match &self.request {
                    WorkRequest::Copy(copy) => copy.source_version,
                    WorkRequest::Upload(upload) => upload.version,
                    WorkRequest::Download(_) => None,
                };
                Ok(IoCall::child(Unmarked::new((bucket, key), version)))
            }
            Step::Reserve => Ok(IoCall::child(self.reservation()?)),
            Step::Paused => Err(IoError::Unfinished.into()),
        }
    }

    fn advance(&mut self) -> Result<Next<PreparedWork>, CallError> {
        while let Some(step) = self.steps.pop_front() {
            if step == Step::Paused {
                if self.install()?.paused {
                    let paused = "the install is paused";
                    return Err(CallError(HostError::new(ErrorKind::Denied, paused)));
                }
                continue;
            }
            self.current = step;
            return Ok(Next::Call(self.call(step)?));
        }
        Err(IoError::Unfinished.into())
    }

    fn ordinal(&self) -> u32 {
        match &self.request {
            WorkRequest::Copy(copy) => copy.ordinal,
            WorkRequest::Download(download) => download.ordinal,
            WorkRequest::Upload(upload) => upload.ordinal,
        }
    }

    /// The row this request proposes, with what names its arguments.
    fn proposed(&self, install: &PluginInstall) -> Result<WorkRow, CallError> {
        let run_id = self.scope.run.run_id;
        let stored = |error: super::rows::StoreError| CallError(unavailable(error.0));
        let reserved = |kind| WorkRow::reserved(install, kind, run_id, self.now_ms);
        Ok(match &self.request {
            WorkRequest::Copy(copy) => {
                let version = copy.source_version.map(|version| version.to_bytes());
                let parts: [&[u8]; 4] = [
                    b"copy",
                    copy.source.as_bytes(),
                    version.as_ref().map_or(&[], |bytes| &bytes[..]),
                    copy.target.as_bytes(),
                ];
                let mut row = reserved(WorkKind::Copy);
                row.digest = digest(&parts);
                row.source = Some(copy.source.clone());
                row.target = Some(copy.target.clone());
                row.version_id = Some(self.minted);
                row
            }
            WorkRequest::Download(download) => {
                let stable = (
                    &download.context,
                    &download.target,
                    &download.checksums,
                    download.max_bytes,
                );
                let stable = encode(&stable).map_err(stored)?;
                let mut row = reserved(WorkKind::Download);
                row.digest = digest(&[b"download", &stable]);
                row.target = Some(download.target.clone());
                row.context = Some(download.context.clone());
                row.version_id = Some(self.minted);
                row
            }
            WorkRequest::Upload(upload) => {
                let stable = (
                    &upload.context,
                    &upload.source,
                    upload.version,
                    &upload.form,
                );
                let stable = encode(&stable).map_err(stored)?;
                let mut row = reserved(WorkKind::Upload);
                row.digest = digest(&[b"upload", &stable]);
                row.source = Some(upload.source.clone());
                row.context = Some(upload.context.clone());
                row
            }
        })
    }

    fn reservation(&self) -> Result<ReserveWork, CallError> {
        let install = self.install()?;
        let identity = Identity::Call {
            run_id: self.scope.run.run_id,
            ordinal: self.ordinal(),
        };
        let proposed = self.proposed(install)?;
        let marked = match &self.request {
            WorkRequest::Download(download) if download.secret => {
                (self.place.as_ref()).map(|(bucket, key)| (bucket.as_str(), key.as_str()))
            }
            _ => None,
        };
        reserve((install, &identity), proposed, marked).map_err(|error| self.refused(error))
    }

    fn refused(&self, error: WorkError) -> CallError {
        CallError(match error {
            WorkError::Conflict => {
                let what = match self.request {
                    WorkRequest::Copy(_) => "a copy",
                    WorkRequest::Download(_) => "a download",
                    WorkRequest::Upload(_) => "an upload",
                };
                let message = format!("this call started {what} with other arguments");
                HostError::new(ErrorKind::Conflict, message)
            }
            WorkError::Store(error) => unavailable(error.0),
        })
    }

    /// What the reserved row needs next.
    fn prepared(&self, row: WorkRow) -> Result<PreparedWork, CallError> {
        if row.state != WorkState::Reserved {
            return Ok(PreparedWork::Known(row.work_id));
        }
        let install = self.install()?;
        let version_id = row.version_id.unwrap_or(row.work_id);
        let action = match self.request.clone() {
            WorkRequest::Copy(copy) => PluginWorkAction::Copy {
                source: copy.source,
                source_version: copy.source_version,
                target: copy.target,
                version_id,
            },
            WorkRequest::Download(download) => PluginWorkAction::Download {
                target: download.target,
                version_id,
                checksums: download.checksums,
                max_bytes: download.max_bytes,
            },
            WorkRequest::Upload(upload) => PluginWorkAction::Upload {
                source: upload.source,
                version: upload.version,
                form: upload.form,
            },
        };
        Ok(PreparedWork::Submit {
            account: install.account,
            spec: Box::new(PluginWorkSpec {
                realm_id: self.scope.realm_id,
                install_id: install.install_id,
                work_id: row.work_id,
                group_id: install.group_id,
                action,
            }),
        })
    }
}

impl Operation for PrepareWork {
    type Output = PreparedWork;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<PreparedWork>, CallError> {
        Ok(Next::Call(self.call(Step::Check)?))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PreparedWork>, CallError> {
        match self.current {
            Step::Check => {
                self.install = Some(result.child::<PluginInstall, CallError>()??);
            }
            Step::CopyGrants => {
                result.child::<BTreeSet<String>, CallError>()??;
            }
            Step::Connection | Step::DownloadGrant | Step::UploadGrant => {
                result.child::<(), CallError>()??;
            }
            Step::Source | Step::Target => {
                self.place = Some(result.child::<(String, String), CallError>()??);
            }
            Step::SourceVersion => {
                let version = result.child::<Option<Ulid>, CallError>()??;
                // Every attempt checks the version it sends again; a copy's missing source fails
                // there, a missing upload source now.
                if version.is_none() && matches!(self.request, WorkRequest::Upload(_)) {
                    let message = format!("no object at {}", self.source());
                    return Err(CallError(HostError::new(ErrorKind::NotFound, message)));
                }
            }
            Step::Reserve => {
                let reserved = result.child::<(WorkRow, Reserve), WorkError>()?;
                let (row, _) = reserved.map_err(|error| self.refused(error))?;
                return Ok(Next::Done(self.prepared(row)?));
            }
            Step::Paused => return Err(IoError::Unfinished.into()),
        }
        self.advance()
    }
}

/// BLAKE3 of length-prefixed parts, so no two argument lists share bytes.
pub fn digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

#[cfg(test)]
#[path = "submit_tests.rs"]
pub(crate) mod tests;
