//! Admission of NFS peers: enrolled hosts by key and network, mount bindings with their live
//! credential and target, and the recheck of each object use before a cached reply is resent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::IpAddr;
use std::time::SystemTime;

use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::vfs::{
    EnrolledHostRecord, HandleKind, NfsMountBinding, NfsMountTarget,
};
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_data::s3::access::admit::{
    AdmitOwnerConfig, AdmitOwnerOperation, check_credential_usable, is_local_access,
};
use aruna_ops_data::s3::access::get::GetAccessOperation;
use aruna_ops_data::vfs::path::children_prefix;
use aruna_ops_data::vfs::policy::{
    AuthorizeVfsOperation, VfsAccess, VfsAccessContext, VfsGrant, VfsRequest,
};
use aruna_ops_data::vfs::{
    FindHostOperation, GetHostOperation, LookupMountOperation, LoopbackHostOperation, VfsError,
};
use aruna_ops_work::jobs::workflow::input_view::{view_bucket, view_job};
use aruna_runner::driver::drive;
use ipnet::IpNet;
use tracing::debug;
use ulid::Ulid;

use super::exports::object_key;
use super::{ArunaNfsBackend, LIVE_ROOT, job_view, vfs_status};
use crate::backend::{HostIdentity, IoState, MountExport, ReplayAccess, ReplayCheck, ReplayGrant};
use crate::types::NfsStat4;

/// Whether `peer` lies in one of `networks`; a malformed network admits nothing.
pub fn network_allows(networks: &[String], peer: IpAddr) -> bool {
    let peer = peer.to_canonical();
    let mut parsed = networks
        .iter()
        .filter_map(|network| network.parse::<IpNet>().ok());
    parsed.any(|network| network.contains(&peer))
}

/// An enrolled host serves only while it is not revoked and the peer is in its networks.
pub(super) fn host_allows(host: &EnrolledHostRecord, peer: IpAddr) -> bool {
    host.revoked_at_ms.is_none() && network_allows(&host.allowed_networks, peer)
}

/// A TLS connection keeps serving only while its proven key is still enrolled for the host.
pub(super) fn key_allows(host: &EnrolledHostRecord, identity: &HostIdentity) -> bool {
    identity
        .fingerprint
        .is_none_or(|pin| host.fingerprints.contains(&pin))
}

/// A binding without networks follows its host's; otherwise the peer must be in one.
pub(super) fn binding_allows(binding: &NfsMountBinding, peer: IpAddr) -> bool {
    binding.allowed_networks.is_empty() || network_allows(&binding.allowed_networks, peer)
}

/// The handle incarnation of a job input view, fixed by its job id.
pub(super) fn view_incarnation(job_id: JobId) -> u128 {
    u128::from_be_bytes(job_id.to_bytes())
}

impl ArunaNfsBackend {
    /// The access context of one request through `binding`, at the current time.
    pub(super) fn access_context(&self, binding: &NfsMountBinding) -> VfsAccessContext {
        VfsAccessContext {
            realm_id: self.realm_id,
            node_id: self.node_id,
            access_key: binding.access_key.clone(),
            protocol: VfsProtocol::Nfs,
            target: binding.target.clone(),
            read_only: binding.read_only,
            now_ms: aruna_clock::unix_timestamp_millis(),
        }
    }

    /// Admits `access` on the export root of a bucket or folder binding from live state.
    pub(super) async fn authorize_root(
        &self,
        binding: &NfsMountBinding,
        access: VfsAccess,
    ) -> Result<VfsGrant, VfsError> {
        let (bucket, key) = match &binding.target {
            NfsMountTarget::Bucket { bucket, .. } => (bucket.as_str(), None),
            NfsMountTarget::Folder { bucket, prefix, .. } => (bucket.as_str(), Some(prefix)),
            NfsMountTarget::JobInputView { .. } => return Err(VfsError::Denied),
        };
        let mut request = VfsRequest::new(access, bucket, key.map(String::as_str));
        if access == VfsAccess::List {
            request.prefix = Some(key.cloned().unwrap_or_default());
        }
        let operation = AuthorizeVfsOperation::new(self.access_context(binding), request);
        drive(operation, &self.context).await
    }

    pub(super) async fn admit_host(
        &self,
        fingerprint: [u8; 32],
        peer: IpAddr,
    ) -> Option<HostIdentity> {
        let host = match drive(FindHostOperation::new(fingerprint), &self.context).await {
            Ok(host) => host?,
            Err(error) => {
                debug!(error = %error, "NFS host lookup failed");
                return None;
            }
        };
        host_allows(&host, peer).then_some(HostIdentity {
            host_id: host.host_id,
            owner: host.owner,
            fingerprint: Some(fingerprint),
        })
    }

    /// The one unrevoked host whose networks hold the loopback `peer`; none or several admit
    /// nothing.
    pub(super) async fn admit_loopback(&self, peer: IpAddr) -> Option<HostIdentity> {
        if !peer.is_loopback() {
            return None;
        }
        let host = match drive(LoopbackHostOperation::new(peer), &self.context).await {
            Ok(host) => host?,
            Err(error) => {
                debug!(error = %error, "NFS loopback host lookup failed");
                return None;
            }
        };
        host_allows(&host, peer).then_some(HostIdentity {
            host_id: host.host_id,
            owner: host.owner,
            fingerprint: None,
        })
    }

    /// The live binding `mount_id` of `host` as one compound uses it: the host and binding
    /// again, the peer networks, the credential and the export target.
    pub(super) async fn admit_mount(
        &self,
        host: &HostIdentity,
        peer: IpAddr,
        mount_id: Ulid,
    ) -> Result<MountExport, NfsStat4> {
        let record = drive(
            GetHostOperation::new(host.host_id, host.owner),
            &self.context,
        )
        .await
        .map_err(|error| hidden_status(&error))?;
        if !host_allows(&record, peer) || !key_allows(&record, host) {
            return Err(NfsStat4::Access);
        }
        let binding = drive(LookupMountOperation::new(mount_id), &self.context)
            .await
            .map_err(|error| vfs_status(&error))?
            .ok_or(NfsStat4::Stale)?;
        if binding.host_id != host.host_id || binding.owner != host.owner {
            return Err(NfsStat4::Stale);
        }
        if !binding_allows(&binding, peer) {
            return Err(NfsStat4::Access);
        }
        let export = match &binding.target {
            NfsMountTarget::Bucket {
                bucket_incarnation, ..
            }
            | NfsMountTarget::Folder {
                bucket_incarnation, ..
            } => {
                self.authorize_root(&binding, VfsAccess::Head)
                    .await
                    .map_err(|error| hidden_status(&error))?;
                export_of(&binding, *bucket_incarnation, LIVE_ROOT, false)
            }
            NfsMountTarget::JobInputView { job_id } => {
                self.admit_attempt(&binding, *job_id).await?;
                self.view(*job_id).await?;
                let root = aruna_ops_data::vfs::path::JOB_ROOT;
                export_of(&binding, view_incarnation(*job_id), root, true)
            }
        };
        self.tables.lock().mounts.insert(mount_id, binding);
        Ok(export)
    }

    /// A job view admits only the live, local credential of the attempt that owns the view.
    pub(super) async fn admit_attempt(
        &self,
        binding: &NfsMountBinding,
        job_id: JobId,
    ) -> Result<(), NfsStat4> {
        if view_job(&binding.access_key, &view_bucket(job_id)) != Some(job_id) {
            return Err(NfsStat4::Access);
        }
        let read = GetAccessOperation::new(binding.access_key.clone());
        let access = drive(read, &self.context)
            .await
            .map_err(|_| NfsStat4::Access)?;
        let usable = check_credential_usable(&access, SystemTime::now()).is_ok();
        if !is_local_access(&access) || !usable {
            return Err(NfsStat4::Access);
        }
        let admit = AdmitOwnerOperation::new(AdmitOwnerConfig {
            realm_id: self.realm_id,
            access_key: access.access_key.clone(),
            user_id: access.user_identity,
            issued_by: access.issued_by,
            revoke: true,
        });
        drive(admit, &self.context)
            .await
            .map_err(|_| NfsStat4::Access)
    }

    /// Before a cached reply leaves again: the host, its key and peer network, every binding
    /// and its credential, then each object use the reply carries, admitted from live state.
    pub(super) async fn recheck_grant(&self, grant: &ReplayGrant) -> Result<(), NfsStat4> {
        let host = GetHostOperation::new(grant.host.host_id, grant.host.owner);
        let record = drive(host, &self.context)
            .await
            .map_err(|_| NfsStat4::Access)?;
        if !host_allows(&record, grant.peer) || !key_allows(&record, &grant.host) {
            return Err(NfsStat4::Access);
        }
        for mount_id in &grant.mounts {
            self.admit_mount(&grant.host, grant.peer, *mount_id)
                .await
                .map_err(|_| NfsStat4::Access)?;
        }
        for check in &grant.checks {
            if !grant.mounts.contains(&check.object.mount_id) {
                return Err(NfsStat4::Access);
            }
            let binding = self
                .tables
                .lock()
                .mounts
                .get(&check.object.mount_id)
                .cloned();
            let binding = binding.ok_or(NfsStat4::Access)?;
            let admitted = match job_view(&binding) {
                true => self.recheck_view(&binding, check).await,
                false => self.recheck_live(&binding, check).await,
            };
            admitted.map_err(|status| {
                debug!(?status, "An NFS replay lost its object admission");
                NfsStat4::Access
            })?;
        }
        Ok(())
    }

    /// One object use of a bucket or folder export, as a fresh request would admit it; a read
    /// runs again through the live read path with its exact range.
    pub(super) async fn recheck_live(
        &self,
        binding: &NfsMountBinding,
        check: &ReplayCheck,
    ) -> Result<(), NfsStat4> {
        let object = check.object;
        match check.access {
            ReplayAccess::Head => self.live_attributes(binding, object).await.map(|_| ()),
            ReplayAccess::Read { offset, length } => {
                let read = self.live_read(binding, object, &IoState::Bypass, offset, length);
                read.await.map(|_| ())
            }
            ReplayAccess::List => {
                let located = self.directory_of(binding, object).await?;
                let prefix = Some(children_prefix(&located.key));
                let listed = self.authorize(binding, VfsAccess::List, None, prefix).await;
                listed.map(|_| ()).map_err(|error| vfs_status(&error))
            }
            ReplayAccess::Write => {
                if binding.read_only {
                    return Err(NfsStat4::ReadOnly);
                }
                let located = self.locate(binding, object).await?;
                let key = match located.kind {
                    HandleKind::File => object_key(&located.key, located.kind),
                    HandleKind::Directory if located.key.is_empty() => None,
                    HandleKind::Directory => Some(children_prefix(&located.key)),
                };
                let written = self.authorize(binding, VfsAccess::Put, key, None).await;
                written.map(|_| ()).map_err(|error| vfs_status(&error))
            }
        }
    }

    /// One object use of a job input view: the run, the attempt credential and the entry's
    /// visibility to the run's creator, and for a read the exact captured bytes again.
    pub(super) async fn recheck_view(
        &self,
        binding: &NfsMountBinding,
        check: &ReplayCheck,
    ) -> Result<(), NfsStat4> {
        let object = check.object;
        match check.access {
            ReplayAccess::Head | ReplayAccess::List => {
                self.view_attributes(binding, object).await.map(|_| ())
            }
            ReplayAccess::Read { offset, length } => {
                let read = self.view_read(binding, object, offset, length);
                read.await.map(|_| ())
            }
            ReplayAccess::Write => Err(NfsStat4::ReadOnly),
        }
    }
}

/// The export a binding is served as.
fn export_of(
    binding: &NfsMountBinding,
    incarnation: u128,
    root: u64,
    persistent: bool,
) -> MountExport {
    MountExport {
        mount_id: binding.mount_id,
        host_id: binding.host_id,
        principal: binding.owner,
        access_key: binding.access_key.clone(),
        incarnation,
        root,
        read_only: binding.read_only || persistent,
        persistent,
    }
}

/// Admission refusals stay access refusals; a gone bucket or binding is stale.
fn hidden_status(error: &VfsError) -> NfsStat4 {
    match error {
        VfsError::StaleHandle | VfsError::NotFound => NfsStat4::Stale,
        VfsError::Unavailable(_) | VfsError::Conflict => NfsStat4::Delay,
        VfsError::Storage(_) => NfsStat4::ServerFault,
        _ => NfsStat4::Access,
    }
}
