//! Filehandle and file operations of a compound: PUTFH with deferred FHEXPIRED, the
//! nonenumerable pseudo-root, export confinement, attributes, I/O, names and READDIR.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;
use ulid::Ulid;

use crate::arguments::{CreateKind, ReaddirArgs};
use crate::attributes::{
    AttributeView, ClientAttributes, decode_change, requests_write_only, same_values, write_bitmap,
    write_fattr,
};
use crate::backend::{
    HandleStatus, IoState, NfsBackend, ObjectAttributes, ObjectRef, ReplayAccess,
};
use crate::compound::{Context, Current, Engine, OpError, OpResult, Target, put_change};
use crate::filehandle::{Filehandle, FilehandleKind, MOUNTS_FILEID, MOUNTS_NAME, ROOT_FILEID};
use crate::rpc::flavor;
use crate::types::{
    IO_LIMIT, NAME_LIMIT, NfsStat4, NfsTime, NfsType4, SpecialStateid, StableHow, Stateid, access,
    expire_type, share,
};
use crate::xdr::XdrWriter;

/// The fsid of the pseudo directories, distinct from every binding's fsid.
const PSEUDO_FSID: (u64, u64) = (0, 1);
/// Bytes of a READDIR reply besides its entries: verifier, list end and eof.
const READDIR_MINIMUM: usize = 16;
/// Most entries one READDIR asks the backend for.
const PAGE_LIMIT: usize = 1024;

/// Checks one component name: nonempty, at most 255 bytes, UTF-8, not `.` or `..`, and no
/// separator or NUL.
pub fn component(raw: &Bytes) -> Result<&str, NfsStat4> {
    if raw.is_empty() {
        return Err(NfsStat4::Invalid);
    }
    if raw.len() > NAME_LIMIT {
        return Err(NfsStat4::NameTooLong);
    }
    let name = std::str::from_utf8(raw).map_err(|_| NfsStat4::Invalid)?;
    if name == "." || name == ".." {
        return Err(NfsStat4::BadName);
    }
    if name.contains(['/', '\0']) {
        return Err(NfsStat4::BadChar);
    }
    Ok(name)
}

/// Hides why a binding is unavailable from a lookup under `/m`; only transient errors pass.
fn hidden(status: NfsStat4) -> NfsStat4 {
    match status {
        NfsStat4::Delay | NfsStat4::ServerFault | NfsStat4::Io => status,
        _ => NfsStat4::NoEntry,
    }
}

fn pseudo_attributes(fileid: u64) -> ObjectAttributes {
    ObjectAttributes {
        kind: NfsType4::Directory,
        size: 4096,
        change: 1,
        fileid,
        mode: 0o555,
        links: 2,
        uid: 0,
        gid: 0,
        space_used: 4096,
        accessed: NfsTime::default(),
        modified: NfsTime::default(),
        changed: NfsTime::default(),
    }
}

/// The attributes of a target plus what their encoding needs beyond them.
pub struct Described {
    pub attributes: ObjectAttributes,
    pub handle: [u8; crate::filehandle::HANDLE_SIZE],
    pub fsid: (u64, u64),
    pub expire: u32,
}

impl Described {
    pub fn view(&self, lease_secs: u32) -> AttributeView<'_> {
        AttributeView {
            attributes: &self.attributes,
            handle: &self.handle,
            fsid: self.fsid,
            expire: self.expire,
            lease_secs,
            mounted_on: self.attributes.fileid,
        }
    }
}

fn expire_of(persistent: bool) -> u32 {
    if persistent {
        expire_type::PERSISTENT
    } else {
        expire_type::VOLATILE_ANY | expire_type::NOEXPIRE_WITH_OPEN
    }
}

impl<B: NfsBackend> Engine<B> {
    fn lease_secs(&self) -> u32 {
        u32::try_from(self.lease.as_secs()).unwrap_or(u32::MAX)
    }

    /// PUTFH: verifies format and MAC before any lookup, the live binding of this host and
    /// the incarnation, then installs a live or an expired handle.
    pub(crate) async fn putfh(&self, ctx: &mut Context<'_>, raw: Bytes) -> OpResult {
        let handle = self.key.open(&raw)?;
        let current = match handle.kind {
            FilehandleKind::PseudoRoot => Current::Root,
            FilehandleKind::Mounts => Current::Mounts,
            FilehandleKind::ExportRoot | FilehandleKind::Object => {
                let export =
                    self.export(ctx, handle.mount_id)
                        .await
                        .map_err(|status| match status {
                            NfsStat4::Delay | NfsStat4::ServerFault => status,
                            _ => NfsStat4::Stale,
                        })?;
                if handle.incarnation != export.incarnation
                    || handle.is_persistent() != export.persistent
                {
                    return Err(NfsStat4::Stale.into());
                }
                if (handle.kind == FilehandleKind::ExportRoot) != (handle.object_id == export.root)
                {
                    return Err(NfsStat4::BadHandle.into());
                }
                let object = ObjectRef {
                    mount_id: handle.mount_id,
                    object_id: handle.object_id,
                };
                let caller = ctx.caller(&export);
                let expired = match self.backend.resolve(&caller, object).await {
                    HandleStatus::Live => false,
                    HandleStatus::Expired => true,
                    HandleStatus::Stale => return Err(NfsStat4::Stale.into()),
                };
                Current::Object {
                    export,
                    object,
                    expired,
                }
            }
        };
        ctx.set_current(current);
        Ok(())
    }

    pub(crate) async fn lookup(&self, ctx: &mut Context<'_>, name: &Bytes) -> OpResult {
        let target = ctx.target()?;
        let name = component(name)?;
        let current = match target {
            Target::Root if name == MOUNTS_NAME => Current::Mounts,
            Target::Root => return Err(NfsStat4::NoEntry.into()),
            Target::Mounts => {
                let mount_id = Ulid::from_string(name).map_err(|_| NfsStat4::NoEntry)?;
                let export = self.export(ctx, mount_id).await.map_err(hidden)?;
                let object = ObjectRef {
                    mount_id,
                    object_id: export.root,
                };
                Current::Object {
                    export,
                    object,
                    expired: false,
                }
            }
            Target::Object { export, object } => {
                let caller = ctx.caller(&export);
                let found = self.backend.lookup(&caller, object, name).await?;
                if found.mount_id != export.mount_id {
                    return Err(NfsStat4::Access.into());
                }
                ctx.grant.record(found, ReplayAccess::Head);
                Current::Object {
                    export,
                    object: found,
                    expired: false,
                }
            }
        };
        ctx.set_current(current);
        Ok(())
    }

    /// LOOKUPP stays inside the export; above its root lie only the pseudo directories.
    pub(crate) async fn lookup_parent(&self, ctx: &mut Context<'_>) -> OpResult {
        let current = match ctx.target()? {
            Target::Root => return Err(NfsStat4::NoEntry.into()),
            Target::Mounts => Current::Root,
            Target::Object { export, object } if object.object_id == export.root => Current::Mounts,
            Target::Object { export, object } => {
                let caller = ctx.caller(&export);
                let parent = self.backend.parent(&caller, object).await?;
                if parent.mount_id != export.mount_id {
                    return Err(NfsStat4::Access.into());
                }
                ctx.grant.record(parent, ReplayAccess::Head);
                Current::Object {
                    export,
                    object: parent,
                    expired: false,
                }
            }
        };
        ctx.set_current(current);
        Ok(())
    }

    /// ACCESS. The pseudo directories grant only traversal, so their entries never list.
    pub(crate) async fn check_access(
        &self,
        ctx: &mut Context<'_>,
        requested: u32,
        out: &mut XdrWriter,
    ) -> OpResult {
        let requested = requested & access::ALL;
        let allowed = match ctx.target()? {
            Target::Root | Target::Mounts => requested & access::LOOKUP,
            Target::Object { export, object } => {
                let caller = ctx.caller(&export);
                let mut allowed =
                    self.backend.access(&caller, object, requested).await? & requested;
                if export.read_only {
                    allowed &= !access::MUTATING;
                }
                ctx.grant.record(object, ReplayAccess::Head);
                allowed
            }
        };
        out.put_u32(requested);
        out.put_u32(allowed);
        Ok(())
    }

    pub(crate) async fn describe(
        &self,
        ctx: &mut Context<'_>,
        target: &Target,
    ) -> Result<Described, NfsStat4> {
        let handle = self.seal(target);
        let described = match target {
            Target::Root => Described {
                attributes: pseudo_attributes(ROOT_FILEID),
                handle,
                fsid: PSEUDO_FSID,
                expire: expire_type::PERSISTENT,
            },
            Target::Mounts => Described {
                attributes: pseudo_attributes(MOUNTS_FILEID),
                handle,
                fsid: PSEUDO_FSID,
                expire: expire_type::PERSISTENT,
            },
            Target::Object { export, object } => {
                let caller = ctx.caller(export);
                let attributes = self.backend.attributes(&caller, *object).await?;
                ctx.grant.record(*object, ReplayAccess::Head);
                Described {
                    attributes,
                    handle,
                    fsid: export.fsid(),
                    expire: expire_of(export.persistent),
                }
            }
        };
        Ok(described)
    }

    pub(crate) async fn getattr(
        &self,
        ctx: &mut Context<'_>,
        request: &[u32],
        out: &mut XdrWriter,
    ) -> OpResult {
        if requests_write_only(request) {
            return Err(NfsStat4::Invalid.into());
        }
        let target = ctx.target()?;
        let described = self.describe(ctx, &target).await?;
        write_fattr(out, request, &described.view(self.lease_secs()));
        Ok(())
    }

    /// SETATTR always answers its attribute mask, empty on error.
    pub(crate) async fn setattr(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        attributes: &ClientAttributes,
        out: &mut XdrWriter,
    ) -> OpResult {
        match self.apply_change(ctx, stateid, attributes).await {
            Ok(mask) => {
                write_bitmap(out, &mask);
                Ok(())
            }
            Err(status) => {
                write_bitmap(out, &[]);
                Err(OpError { status, keep: true })
            }
        }
    }

    async fn apply_change(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        attributes: &ClientAttributes,
    ) -> Result<Vec<u32>, NfsStat4> {
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::Access);
        };
        let (change, mask) = decode_change(attributes)?;
        if export.read_only {
            return Err(NfsStat4::ReadOnly);
        }
        let state = if change.size.is_some() {
            self.io_state(ctx, stateid, object, share::WRITE)?
        } else {
            IoState::Anonymous
        };
        let caller = ctx.caller(&export);
        self.backend
            .set_attributes(&caller, object, &state, &change)
            .await?;
        ctx.grant.record(object, ReplayAccess::Write);
        Ok(mask)
    }

    /// VERIFY when `equal`, NVERIFY otherwise.
    pub(crate) async fn verify(
        &self,
        ctx: &mut Context<'_>,
        attributes: &ClientAttributes,
        equal: bool,
    ) -> OpResult {
        let target = ctx.target()?;
        let described = self.describe(ctx, &target).await?;
        let same = same_values(attributes, &described.view(self.lease_secs()))?;
        match (equal, same) {
            (true, true) | (false, false) => Ok(()),
            (true, false) => Err(NfsStat4::NotSame.into()),
            (false, true) => Err(NfsStat4::Same.into()),
        }
    }

    /// The state an I/O runs under. Anonymous and bypass stateids carry no open; any other
    /// must be a live open or lock stateid of this client on this file with the access.
    pub(crate) fn io_state(
        &self,
        ctx: &Context<'_>,
        stateid: Stateid,
        object: ObjectRef,
        need: u32,
    ) -> Result<IoState, NfsStat4> {
        match stateid.special() {
            Some(SpecialStateid::Anonymous) => return Ok(IoState::Anonymous),
            Some(SpecialStateid::Bypass) if need == share::READ => return Ok(IoState::Bypass),
            Some(SpecialStateid::Bypass) => return Ok(IoState::Anonymous),
            Some(SpecialStateid::Invalid) => return Err(NfsStat4::BadStateid),
            Some(SpecialStateid::Current) | None => {}
        }
        let stateid = ctx.resolve_stateid(stateid)?;
        if stateid.special().is_some() {
            return Err(NfsStat4::BadStateid);
        }
        let client_id = ctx.client_id()?;
        let tables = self.tables.lock();
        let entry = tables.states.validate(&stateid, client_id)?;
        if entry.object != object {
            return Err(NfsStat4::BadStateid);
        }
        let (open, lock_owner) = match &entry.kind {
            crate::state::StateKind::Open { .. } => (entry, None),
            crate::state::StateKind::Lock { open, .. } => {
                let open = tables.states.get(open).ok_or(NfsStat4::BadStateid)?;
                (open, Some(entry.owner.clone()))
            }
        };
        let crate::state::StateKind::Open { access, .. } = open.kind else {
            return Err(NfsStat4::BadStateid);
        };
        let allowed = if need == share::READ {
            access & share::BOTH != 0
        } else {
            access & need == need
        };
        if !allowed {
            return Err(NfsStat4::OpenMode);
        }
        Ok(IoState::Open {
            owner: open.owner.clone(),
            lock_owner,
        })
    }

    pub(crate) async fn read(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        offset: u64,
        count: u32,
        out: &mut XdrWriter,
    ) -> OpResult {
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::IsDirectory.into());
        };
        let state = self.io_state(ctx, stateid, object, share::READ)?;
        let count = ctx.read_room(out.len(), count.min(IO_LIMIT))?;
        let caller = ctx.caller(&export);
        let reply = self
            .backend
            .read(&caller, object, &state, offset, count)
            .await?;
        let read = ReplayAccess::Read {
            offset,
            length: count,
            source: reply.source.clone(),
        };
        ctx.grant.record(object, read);
        out.put_bool(reply.eof);
        out.put_opaque(&reply.data);
        Ok(())
    }

    pub(crate) async fn write(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        position: (u64, StableHow),
        data: Bytes,
        out: &mut XdrWriter,
    ) -> OpResult {
        let (offset, stable) = position;
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::IsDirectory.into());
        };
        if export.read_only {
            return Err(NfsStat4::ReadOnly.into());
        }
        if offset.checked_add(data.len() as u64).is_none() {
            return Err(NfsStat4::FileTooBig.into());
        }
        let state = self.io_state(ctx, stateid, object, share::WRITE)?;
        let caller = ctx.caller(&export);
        let reply = self
            .backend
            .write(&caller, object, &state, offset, data, stable)
            .await?;
        ctx.grant.record(object, ReplayAccess::Write);
        out.put_u32(reply.count);
        out.put_u32(reply.committed as u32);
        out.put_fixed(&reply.verifier);
        Ok(())
    }

    /// COMMIT carries no stateid; the backend checks the caller's write authority.
    pub(crate) async fn commit(
        &self,
        ctx: &mut Context<'_>,
        offset: u64,
        count: u32,
        out: &mut XdrWriter,
    ) -> OpResult {
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::IsDirectory.into());
        };
        if offset.checked_add(u64::from(count)).is_none() {
            return Err(NfsStat4::Invalid.into());
        }
        let caller = ctx.caller(&export);
        let verifier = self.backend.commit(&caller, object, offset, count).await?;
        ctx.grant.record(object, ReplayAccess::Write);
        out.put_fixed(&verifier);
        Ok(())
    }

    /// CREATE makes directories only; files come from OPEN and other types are unsupported.
    pub(crate) async fn create(
        &self,
        ctx: &mut Context<'_>,
        kind: CreateKind,
        name: &Bytes,
        attributes: &ClientAttributes,
        out: &mut XdrWriter,
    ) -> OpResult {
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::Access.into());
        };
        if kind != CreateKind::Directory {
            return Err(NfsStat4::BadType.into());
        }
        if export.read_only {
            return Err(NfsStat4::ReadOnly.into());
        }
        let name = component(name)?;
        let (change, mask) = decode_change(attributes)?;
        let caller = ctx.caller(&export);
        let reply = self
            .backend
            .create_directory(&caller, object, name, &change)
            .await?;
        if reply.object.mount_id != export.mount_id {
            return Err(NfsStat4::ServerFault.into());
        }
        ctx.grant.record(object, ReplayAccess::Write);
        put_change(out, &reply.change);
        let applied: &[u32] = if reply.created { &mask } else { &[] };
        write_bitmap(out, applied);
        ctx.set_current(Current::Object {
            export,
            object: reply.object,
            expired: false,
        });
        Ok(())
    }

    pub(crate) async fn remove(
        &self,
        ctx: &mut Context<'_>,
        name: &Bytes,
        out: &mut XdrWriter,
    ) -> OpResult {
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::Access.into());
        };
        if export.read_only {
            return Err(NfsStat4::ReadOnly.into());
        }
        let name = component(name)?;
        let caller = ctx.caller(&export);
        let change = self.backend.remove(&caller, object, name).await?;
        ctx.grant.record(object, ReplayAccess::Write);
        put_change(out, &change);
        Ok(())
    }

    /// RENAME from the saved directory to the current one; different exports are XDEV.
    pub(crate) async fn rename(
        &self,
        ctx: &mut Context<'_>,
        old: &Bytes,
        new: &Bytes,
        out: &mut XdrWriter,
    ) -> OpResult {
        let (source_export, source) = match &ctx.saved {
            Current::None => return Err(NfsStat4::NoFilehandle.into()),
            Current::Object { expired: true, .. } => {
                return Err(NfsStat4::FilehandleExpired.into());
            }
            Current::Object { export, object, .. } => (export.clone(), *object),
            Current::Root | Current::Mounts => return Err(NfsStat4::Xdev.into()),
        };
        let Target::Object { export, object } = ctx.target()? else {
            return Err(NfsStat4::Xdev.into());
        };
        if source_export.mount_id != export.mount_id {
            return Err(NfsStat4::Xdev.into());
        }
        if export.read_only {
            return Err(NfsStat4::ReadOnly.into());
        }
        let old = component(old)?;
        let new = component(new)?;
        let caller = ctx.caller(&export);
        let (from, to) = self
            .backend
            .rename(&caller, source, old, object, new)
            .await?;
        ctx.grant.record(source, ReplayAccess::Write);
        ctx.grant.record(object, ReplayAccess::Write);
        put_change(out, &from);
        put_change(out, &to);
        Ok(())
    }

    /// READDIR. Cookies 1 and 2 are reserved, the pseudo directories list nothing, and the
    /// reply honors `maxcount` or answers TOOSMALL when not even one entry fits.
    pub(crate) async fn readdir(
        &self,
        ctx: &mut Context<'_>,
        args: &ReaddirArgs,
        out: &mut XdrWriter,
    ) -> OpResult {
        if requests_write_only(&args.request) {
            return Err(NfsStat4::Invalid.into());
        }
        let target = ctx.target()?;
        if args.cookie == 1 || args.cookie == 2 {
            return Err(NfsStat4::BadCookie.into());
        }
        let maxcount = (args.maxcount as usize).min(ctx.reply_limit().saturating_sub(out.len()));
        if maxcount < READDIR_MINIMUM {
            return Err(NfsStat4::TooSmall.into());
        }
        let Target::Object { export, object } = target else {
            if args.cookie != 0 && args.verifier != [0; 8] {
                return Err(NfsStat4::NotSame.into());
            }
            out.put_fixed(&[0; 8]);
            out.put_bool(false);
            out.put_bool(true);
            return Ok(());
        };
        let limit = (args.dircount.max(args.maxcount) as usize / 32).clamp(1, PAGE_LIMIT);
        let caller = ctx.caller(&export);
        let page = self
            .backend
            .read_directory(&caller, object, args.cookie, args.verifier, limit)
            .await?;
        ctx.grant.record(object, ReplayAccess::List);
        let mut body = XdrWriter::new();
        body.put_fixed(&page.verifier);
        let mut emitted = 0usize;
        let mut complete = true;
        for entry in &page.entries {
            if entry.cookie <= 2 || entry.object.mount_id != export.mount_id {
                continue;
            }
            let handle = self
                .key
                .seal(&Filehandle::object(&export, entry.object.object_id));
            let described = Described {
                attributes: entry.attributes.clone(),
                handle,
                fsid: export.fsid(),
                expire: expire_of(export.persistent),
            };
            let mut item = XdrWriter::new();
            item.put_bool(true);
            item.put_u64(entry.cookie);
            item.put_string(&entry.name);
            write_fattr(&mut item, &args.request, &described.view(self.lease_secs()));
            if body.len() + item.len() + 8 > maxcount {
                complete = false;
                break;
            }
            body.put_raw(item.as_slice());
            emitted += 1;
            // Each name the reply shows must still be visible when the reply is replayed.
            ctx.grant.record(entry.object, ReplayAccess::Head);
        }
        if emitted == 0 && !complete {
            return Err(NfsStat4::TooSmall.into());
        }
        body.put_bool(false);
        body.put_bool(page.eof && complete);
        out.put_raw(body.as_slice());
        Ok(())
    }

    /// SECINFO for a name, or SECINFO_NO_NAME when `name` is `None`. Both advertise AUTH_SYS,
    /// which mutual TLS protects, and consume the current filehandle.
    pub(crate) async fn secinfo(
        &self,
        ctx: &mut Context<'_>,
        name: Option<&Bytes>,
        out: &mut XdrWriter,
    ) -> OpResult {
        let target = ctx.target()?;
        if let Some(name) = name {
            let name = component(name)?;
            match target {
                Target::Root if name == MOUNTS_NAME => {}
                Target::Root => return Err(NfsStat4::NoEntry.into()),
                Target::Mounts => {
                    let mount_id = Ulid::from_string(name).map_err(|_| NfsStat4::NoEntry)?;
                    self.export(ctx, mount_id).await.map_err(hidden)?;
                }
                Target::Object { export, object } => {
                    let caller = ctx.caller(&export);
                    self.backend.lookup(&caller, object, name).await?;
                }
            }
        }
        out.put_u32(1);
        out.put_u32(flavor::SYS);
        ctx.set_current(Current::None);
        Ok(())
    }

    pub(crate) async fn secinfo_style(
        &self,
        ctx: &mut Context<'_>,
        style: u32,
        out: &mut XdrWriter,
    ) -> OpResult {
        match style {
            0 => {}
            1 => {
                if matches!(ctx.target()?, Target::Root) {
                    return Err(NfsStat4::NoEntry.into());
                }
            }
            _ => return Err(NfsStat4::Invalid.into()),
        }
        self.secinfo(ctx, None, out).await
    }
}
