//! OPEN with its create modes and the reject-all-reclaims policy, OPEN_DOWNGRADE, CLOSE,
//! byte-range locks and the TEST_STATEID and FREE_STATEID operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;

use crate::arguments::{LockArgs, Locker, OpenArgs, OpenClaim, OpenCreate};
use crate::attributes::{decode_change, exclusive_settable, write_bitmap};
use crate::backend::{
    AttributeChange, Caller, ChangeInfo, CreateMode, LockDenial, LockError, LockKind, LockRange,
    NfsBackend, ObjectRef, OwnerKey, ReplayAccess, ShareRequest,
};
use crate::compound::{
    Context, Current, Engine, OpError, OpResult, Target, put_change, put_stateid,
};
use crate::files::component;
use crate::state::StateKind;
use crate::types::{NfsStat4, Stateid, share};
use crate::xdr::XdrWriter;

/// `OPEN4_RESULT_LOCKTYPE_POSIX`
const LOCKTYPE_POSIX: u32 = 0x4;
/// `OPEN_DELEGATE_NONE` and `OPEN_DELEGATE_NONE_EXT`.
const DELEGATE_NONE: u32 = 0;
const DELEGATE_EXTENDED: u32 = 3;
/// `WND4_NOT_WANTED` and `WND4_NOT_SUPP_FTYPE`: why no delegation was granted.
const NOT_WANTED: u32 = 0;
const NOT_SUPPORTED: u32 = 3;

/// A lock range must be nonempty and must not run past the largest offset.
fn check_range(range: LockRange) -> Result<(), NfsStat4> {
    if range.length == 0 {
        return Err(NfsStat4::Invalid);
    }
    if range.length != u64::MAX && range.offset.checked_add(range.length).is_none() {
        return Err(NfsStat4::Invalid);
    }
    Ok(())
}

fn put_denial(out: &mut XdrWriter, denial: &LockDenial) {
    out.put_u64(denial.range.offset);
    out.put_u64(denial.range.length);
    out.put_u32(match denial.kind {
        LockKind::Read => 1,
        LockKind::Write => 2,
    });
    out.put_u64(denial.owner.client_id);
    out.put_opaque(&denial.owner.owner);
}

/// A resolved non-special stateid; special ones are BAD_STATEID where an operation needs state.
fn owned_stateid(ctx: &Context<'_>, stateid: Stateid) -> Result<Stateid, NfsStat4> {
    let stateid = ctx.resolve_stateid(stateid)?;
    if stateid.special().is_some() {
        return Err(NfsStat4::BadStateid);
    }
    Ok(stateid)
}

/// The current filehandle as a file object; state never lives on a pseudo directory.
fn object_target(ctx: &Context<'_>) -> Result<(Caller, ObjectRef), NfsStat4> {
    match ctx.target()? {
        Target::Object { export, object } => Ok((ctx.caller(&export), object)),
        Target::Root | Target::Mounts => Err(NfsStat4::IsDirectory),
    }
}

impl<B: NfsBackend> Engine<B> {
    /// OPEN. Reclaims get NO_GRACE because no state survives a restart; fresh opens and
    /// creates run under current admission.
    pub(crate) async fn open(
        &self,
        ctx: &mut Context<'_>,
        args: OpenArgs,
        out: &mut XdrWriter,
    ) -> OpResult {
        let client_id = ctx.client_id()?;
        let access = args.access & share::BOTH;
        let want = args.access & share::WANT_MASK;
        if access == 0
            || args.deny > share::BOTH
            || args.access & !(share::BOTH | share::WANT_MASK) != 0
        {
            return Err(NfsStat4::Invalid.into());
        }
        let creating = !matches!(args.create, OpenCreate::None);
        let name = match &args.claim {
            OpenClaim::Previous | OpenClaim::PreviousHandle | OpenClaim::DelegatePrevious => {
                return Err(NfsStat4::NoGrace.into());
            }
            OpenClaim::DelegateCurrent | OpenClaim::DelegateHandle => {
                return Err(NfsStat4::BadStateid.into());
            }
            OpenClaim::Handle if creating => return Err(NfsStat4::Invalid.into()),
            OpenClaim::Handle => None,
            OpenClaim::Null(name) => Some(name.clone()),
        };
        let (export, base) = match ctx.target()? {
            Target::Object { export, object } => (export, object),
            Target::Root | Target::Mounts if name.is_none() => {
                return Err(NfsStat4::IsDirectory.into());
            }
            Target::Root | Target::Mounts if creating => return Err(NfsStat4::Access.into()),
            Target::Root | Target::Mounts => return Err(NfsStat4::NoEntry.into()),
        };
        if export.read_only && (creating || access & share::WRITE != 0) {
            return Err(NfsStat4::ReadOnly.into());
        }
        let caller = ctx.caller(&export);
        let owner = OwnerKey {
            client_id,
            owner: args.owner.clone(),
        };
        // A created file holds the requested reservation from its creation on.
        let requested = ShareRequest {
            owner: owner.clone(),
            access,
            deny: args.deny,
        };
        let (object, change, applied) = match name {
            None => (base, ChangeInfo::default(), Vec::new()),
            Some(name) => {
                let create = (&args.create, &requested);
                self.open_name(&caller, base, &name, create).await?
            }
        };
        if object.mount_id != export.mount_id {
            return Err(NfsStat4::ServerFault.into());
        }
        let combined = {
            let tables = self.tables.lock();
            tables
                .states
                .open_modes(&owner, object)
                .map_or((access, args.deny), |(held, denied)| {
                    (held | access, denied | args.deny)
                })
        };
        let share = ShareRequest {
            owner: owner.clone(),
            access: combined.0,
            deny: combined.1,
        };
        self.share_retry(ctx, &caller, object, &share).await?;
        let stateid =
            self.tables
                .lock()
                .states
                .record_open(owner, object, caller, (access, args.deny));
        let used = match access & share::WRITE {
            0 => ReplayAccess::Head,
            _ => ReplayAccess::Write,
        };
        ctx.grant.record(object, used);
        ctx.set_current(Current::Object {
            export,
            object,
            expired: false,
        });
        ctx.current_stateid = Some(stateid);
        put_stateid(out, &stateid);
        put_change(out, &change);
        out.put_u32(LOCKTYPE_POSIX);
        write_bitmap(out, &applied);
        match want {
            0 => out.put_u32(DELEGATE_NONE),
            share::WANT_NONE | share::WANT_CANCEL => {
                out.put_u32(DELEGATE_EXTENDED);
                out.put_u32(NOT_WANTED);
            }
            _ => {
                out.put_u32(DELEGATE_EXTENDED);
                out.put_u32(NOT_SUPPORTED);
            }
        }
        Ok(())
    }

    /// Finds or creates `name` in `directory` per the create mode; returns the object, the
    /// directory change and the attributes a new file received.
    async fn open_name(
        &self,
        caller: &Caller,
        directory: ObjectRef,
        name: &Bytes,
        (create, share): (&OpenCreate, &ShareRequest),
    ) -> Result<(ObjectRef, ChangeInfo, Vec<u32>), NfsStat4> {
        let name = component(name)?;
        let (mode, change, mask) = match create {
            OpenCreate::None => {
                let object = self.backend.lookup(caller, directory, name).await?;
                let directory_change = self.backend.attributes(caller, directory).await?.change;
                let change = ChangeInfo {
                    atomic: false,
                    before: directory_change,
                    after: directory_change,
                };
                return Ok((object, change, Vec::new()));
            }
            OpenCreate::Unchecked(attributes) => {
                let (change, mask) = decode_change(attributes)?;
                (CreateMode::Unchecked, change, mask)
            }
            OpenCreate::Guarded(attributes) => {
                let (change, mask) = decode_change(attributes)?;
                (CreateMode::Guarded, change, mask)
            }
            OpenCreate::Exclusive(verifier) => (
                CreateMode::Exclusive(*verifier),
                AttributeChange::default(),
                Vec::new(),
            ),
            OpenCreate::ExclusiveAttributes(verifier, attributes) => {
                if !exclusive_settable(&attributes.mask) {
                    return Err(NfsStat4::Invalid);
                }
                let (change, mask) = decode_change(attributes)?;
                (CreateMode::Exclusive(*verifier), change, mask)
            }
        };
        let reply = self
            .backend
            .create_file(caller, directory, name, mode, share, &change)
            .await?;
        let applied = if reply.created { mask } else { Vec::new() };
        Ok((reply.object, reply.change, applied))
    }

    /// Sets a share reservation; on conflict it first revokes expired holders, then retries once.
    async fn share_retry(
        &self,
        ctx: &Context<'_>,
        caller: &Caller,
        object: ObjectRef,
        share: &ShareRequest,
    ) -> Result<(), NfsStat4> {
        match self.backend.open(caller, object, share).await {
            Err(NfsStat4::ShareDenied) => {
                self.sweep(ctx.now).await;
                self.backend.open(caller, object, share).await
            }
            other => other,
        }
    }

    /// The live open stateid of this client on the current file, with its owner and modes.
    fn open_entry(
        &self,
        ctx: &Context<'_>,
        stateid: Stateid,
        object: ObjectRef,
    ) -> Result<(Stateid, OwnerKey, u32, u32), NfsStat4> {
        let stateid = owned_stateid(ctx, stateid)?;
        let client_id = ctx.client_id()?;
        let tables = self.tables.lock();
        let entry = tables.states.validate(&stateid, client_id)?;
        if entry.object != object {
            return Err(NfsStat4::BadStateid);
        }
        match entry.kind {
            StateKind::Open { access, deny } => Ok((stateid, entry.owner.clone(), access, deny)),
            StateKind::Lock { .. } => Err(NfsStat4::BadStateid),
        }
    }

    pub(crate) async fn open_downgrade(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        modes: (u32, u32),
        out: &mut XdrWriter,
    ) -> OpResult {
        let (caller, object) = object_target(ctx)?;
        let (stateid, owner, held, denied) = self.open_entry(ctx, stateid, object)?;
        let access = modes.0 & share::BOTH;
        let deny = modes.1;
        if access == 0 || access & !held != 0 || deny & !denied != 0 || modes.0 & !share::BOTH != 0
        {
            return Err(NfsStat4::Invalid.into());
        }
        let share = ShareRequest {
            owner,
            access,
            deny,
        };
        self.backend.open(&caller, object, &share).await?;
        let stateid = self
            .tables
            .lock()
            .states
            .downgrade(&stateid.other, (access, deny))?;
        ctx.grant.record(object, ReplayAccess::Head);
        ctx.current_stateid = Some(stateid);
        put_stateid(out, &stateid);
        Ok(())
    }

    /// CLOSE. A nonzero seqid must be current; open locks must be released first.
    pub(crate) async fn close(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        out: &mut XdrWriter,
    ) -> OpResult {
        let (caller, object) = object_target(ctx)?;
        let (stateid, owner, _, _) = self.open_entry(ctx, stateid, object)?;
        if self.tables.lock().states.has_locks(&stateid.other) {
            return Err(NfsStat4::LocksHeld.into());
        }
        self.backend.close(&caller, object, &owner).await?;
        self.tables.lock().states.close(&stateid.other);
        ctx.grant.record(object, ReplayAccess::Head);
        ctx.current_stateid = Some(Stateid::INVALID);
        put_stateid(out, &Stateid::INVALID);
        Ok(())
    }

    /// Takes a lock; on conflict it first revokes expired holders, then retries once.
    async fn lock_retry(
        &self,
        ctx: &Context<'_>,
        caller: &Caller,
        object: ObjectRef,
        owner: &OwnerKey,
        request: (LockKind, LockRange),
    ) -> Result<(), LockError> {
        let (kind, range) = request;
        match self.backend.lock(caller, object, owner, kind, range).await {
            Err(LockError::Denied(_)) => {
                self.sweep(ctx.now).await;
                self.backend.lock(caller, object, owner, kind, range).await
            }
            other => other,
        }
    }

    /// LOCK. Reclaims get NO_GRACE; a new lock owner starts from a live open stateid.
    pub(crate) async fn lock(
        &self,
        ctx: &mut Context<'_>,
        args: LockArgs,
        out: &mut XdrWriter,
    ) -> OpResult {
        if args.reclaim {
            return Err(NfsStat4::NoGrace.into());
        }
        check_range(args.range)?;
        let (caller, object) = object_target(ctx)?;
        let client_id = ctx.client_id()?;
        let (lock_other, lock_owner, access) = match &args.locker {
            Locker::New { open, owner } => {
                let (open, _, access, _) = self.open_entry(ctx, *open, object)?;
                let lock_owner = OwnerKey {
                    client_id,
                    owner: owner.clone(),
                };
                let other = self.tables.lock().states.lock_state(
                    open.other,
                    lock_owner.clone(),
                    object,
                    caller.clone(),
                );
                (other, lock_owner, access)
            }
            Locker::Existing(stateid) => self.lock_entry(ctx, *stateid, object)?,
        };
        let needed = match args.kind {
            LockKind::Read => share::READ,
            LockKind::Write => share::WRITE,
        };
        if access & needed == 0 {
            return Err(NfsStat4::OpenMode.into());
        }
        match self
            .lock_retry(ctx, &caller, object, &lock_owner, (args.kind, args.range))
            .await
        {
            Ok(()) => {}
            Err(LockError::Denied(denial)) => {
                put_denial(out, &denial);
                return Err(OpError {
                    status: NfsStat4::Denied,
                    keep: true,
                });
            }
            Err(LockError::Status(status)) => return Err(status.into()),
        }
        let stateid = self.tables.lock().states.mark_lock(&lock_other, true)?;
        ctx.grant.record(object, ReplayAccess::Head);
        ctx.current_stateid = Some(stateid);
        put_stateid(out, &stateid);
        Ok(())
    }

    /// A live lock stateid of this client on the current file, its owner and its open access.
    fn lock_entry(
        &self,
        ctx: &Context<'_>,
        stateid: Stateid,
        object: ObjectRef,
    ) -> Result<([u8; 12], OwnerKey, u32), NfsStat4> {
        let stateid = owned_stateid(ctx, stateid)?;
        let client_id = ctx.client_id()?;
        let tables = self.tables.lock();
        let entry = tables.states.validate(&stateid, client_id)?;
        if entry.object != object {
            return Err(NfsStat4::BadStateid);
        }
        let StateKind::Lock { open, .. } = entry.kind else {
            return Err(NfsStat4::BadStateid);
        };
        let access = match tables.states.get(&open).map(|open| &open.kind) {
            Some(StateKind::Open { access, .. }) => *access,
            _ => return Err(NfsStat4::BadStateid),
        };
        Ok((stateid.other, entry.owner.clone(), access))
    }

    /// LOCKT names its owner directly and carries no stateid.
    pub(crate) async fn lock_test(
        &self,
        ctx: &mut Context<'_>,
        kind: LockKind,
        range: LockRange,
        owner: Bytes,
        out: &mut XdrWriter,
    ) -> OpResult {
        check_range(range)?;
        let (caller, object) = object_target(ctx)?;
        let owner = OwnerKey {
            client_id: ctx.client_id()?,
            owner,
        };
        let mut result = self
            .backend
            .test_lock(&caller, object, &owner, kind, range)
            .await;
        if matches!(result, Err(LockError::Denied(_))) {
            self.sweep(ctx.now).await;
            result = self
                .backend
                .test_lock(&caller, object, &owner, kind, range)
                .await;
        }
        match result {
            Ok(()) => Ok(()),
            Err(LockError::Denied(denial)) => {
                put_denial(out, &denial);
                Err(OpError {
                    status: NfsStat4::Denied,
                    keep: true,
                })
            }
            Err(LockError::Status(status)) => Err(status.into()),
        }
    }

    pub(crate) async fn unlock(
        &self,
        ctx: &mut Context<'_>,
        stateid: Stateid,
        range: LockRange,
        out: &mut XdrWriter,
    ) -> OpResult {
        check_range(range)?;
        let (caller, object) = object_target(ctx)?;
        let (other, owner, _) = self.lock_entry(ctx, stateid, object)?;
        let remaining = self.backend.unlock(&caller, object, &owner, range).await?;
        let stateid = self.tables.lock().states.mark_lock(&other, remaining)?;
        ctx.grant.record(object, ReplayAccess::Head);
        ctx.current_stateid = Some(stateid);
        put_stateid(out, &stateid);
        Ok(())
    }

    pub(crate) fn test_stateids(
        &self,
        ctx: &mut Context<'_>,
        stateids: &[Stateid],
        out: &mut XdrWriter,
    ) -> OpResult {
        let client_id = ctx.client_id()?;
        let tables = self.tables.lock();
        out.put_u32(stateids.len() as u32);
        for stateid in stateids {
            let status = if stateid.special().is_some() {
                NfsStat4::BadStateid
            } else {
                tables.states.test(stateid, client_id)
            };
            out.put_u32(status.code());
        }
        Ok(())
    }

    pub(crate) fn free_stateid(&self, ctx: &mut Context<'_>, stateid: Stateid) -> OpResult {
        let stateid = owned_stateid(ctx, stateid)?;
        let client_id = ctx.client_id()?;
        self.tables.lock().states.free(&stateid, client_id)?;
        Ok(())
    }
}
