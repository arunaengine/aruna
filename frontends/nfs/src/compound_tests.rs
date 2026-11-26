//! Protocol tests of the COMPOUND engine against the in-memory backend: pseudo-root, slots and
//! replay, reclaims, deferred FHEXPIRED, leases, revocation, stateids, locks and READDIR.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::Ipv4Addr;

use super::*;
use crate::filehandle::HANDLE_SIZE;
use crate::mock::{DIRECTORY_VERIFIER, Fixture, MockBackend, ROOT, WRITE_VERIFIER};
use crate::types::{access, attribute, sequence_flag, share};

const LEASE: Duration = Duration::from_secs(90);

fn principal() -> RpcPrincipal {
    RpcPrincipal::Sys {
        uid: 1000,
        gid: 1000,
    }
}

/// One compound under construction.
struct Ops {
    writer: XdrWriter,
    count: u32,
}

fn put_id(writer: &mut XdrWriter, stateid: &Stateid) {
    writer.put_u32(stateid.seqid);
    writer.put_fixed(&stateid.other);
}

fn empty_fattr(writer: &mut XdrWriter) {
    writer.put_u32(0);
    writer.put_u32(0);
}

/// Channel attributes asking for `cached` bytes of cached reply.
fn put_channel(writer: &mut XdrWriter, cached: u32) {
    for value in [0, 1 << 20, 1 << 20, cached, 16, 4, 0] {
        writer.put_u32(value);
    }
}

impl Ops {
    fn new() -> Self {
        Self {
            writer: XdrWriter::new(),
            count: 0,
        }
    }

    fn op(mut self, code: NfsOp4, arguments: impl FnOnce(&mut XdrWriter)) -> Self {
        self.writer.put_u32(code.code());
        arguments(&mut self.writer);
        self.count += 1;
        self
    }

    fn plain(self, code: NfsOp4) -> Self {
        self.op(code, |_| {})
    }

    fn sequence(self, session: [u8; 16], sequence: u32, cache: bool) -> Self {
        self.op(NfsOp4::Sequence, |writer| {
            writer.put_fixed(&session);
            writer.put_u32(sequence);
            writer.put_u32(0);
            writer.put_u32(0);
            writer.put_bool(cache);
        })
    }

    fn putfh(self, handle: &[u8]) -> Self {
        self.op(NfsOp4::Putfh, |writer| writer.put_opaque(handle))
    }

    fn lookup(self, name: &str) -> Self {
        self.op(NfsOp4::Lookup, |writer| writer.put_string(name))
    }

    fn getattr(self, bits: &[u32]) -> Self {
        self.op(NfsOp4::Getattr, |writer| {
            let mut bitmap = Vec::new();
            for bit in bits {
                crate::types::bitmap_set(&mut bitmap, *bit);
            }
            writer.put_u32(bitmap.len() as u32);
            for word in bitmap {
                writer.put_u32(word);
            }
        })
    }

    fn access(self, bits: u32) -> Self {
        self.op(NfsOp4::Access, |writer| writer.put_u32(bits))
    }

    fn readdir(self, cookie: u64, verifier: [u8; 8], maxcount: u32) -> Self {
        self.op(NfsOp4::Readdir, |writer| {
            writer.put_u64(cookie);
            writer.put_fixed(&verifier);
            writer.put_u32(maxcount);
            writer.put_u32(maxcount);
            writer.put_u32(1);
            writer.put_u32(1 << attribute::TYPE);
        })
    }

    /// OPEN of `name` under the current directory; `create` writes the `openflag4`.
    fn open(self, owner: &[u8], name: &str, create: impl FnOnce(&mut XdrWriter)) -> Self {
        self.open_deny(owner, (name, 0), create)
    }

    /// OPEN for reading and writing that denies `deny` to other owners.
    fn open_deny(
        self,
        owner: &[u8],
        (name, deny): (&str, u32),
        create: impl FnOnce(&mut XdrWriter),
    ) -> Self {
        self.op(NfsOp4::Open, |writer| {
            writer.put_u32(0);
            writer.put_u32(share::BOTH);
            writer.put_u32(deny);
            writer.put_u64(0);
            writer.put_opaque(owner);
            create(writer);
            writer.put_u32(0);
            writer.put_string(name);
        })
    }

    fn open_previous(self, owner: &[u8]) -> Self {
        self.op(NfsOp4::Open, |writer| {
            writer.put_u32(0);
            writer.put_u32(share::READ);
            writer.put_u32(0);
            writer.put_u64(0);
            writer.put_opaque(owner);
            writer.put_u32(0);
            writer.put_u32(1);
            writer.put_u32(0);
        })
    }

    fn close(self, stateid: &Stateid) -> Self {
        self.op(NfsOp4::Close, |writer| {
            writer.put_u32(0);
            put_id(writer, stateid);
        })
    }

    fn write(self, stateid: &Stateid, offset: u64, data: &[u8]) -> Self {
        self.op(NfsOp4::Write, |writer| {
            put_id(writer, stateid);
            writer.put_u64(offset);
            writer.put_u32(2);
            writer.put_opaque(data);
        })
    }

    fn read(self, stateid: &Stateid, count: u32) -> Self {
        self.op(NfsOp4::Read, |writer| {
            put_id(writer, stateid);
            writer.put_u64(0);
            writer.put_u32(count);
        })
    }

    /// LOCK of a write range; `open` starts a new lock owner, otherwise `stateid` is a lock stateid.
    fn lock(self, reclaim: bool, length: u64, stateid: &Stateid, open: Option<&[u8]>) -> Self {
        self.op(NfsOp4::Lock, |writer| {
            writer.put_u32(2);
            writer.put_bool(reclaim);
            writer.put_u64(0);
            writer.put_u64(length);
            match open {
                Some(owner) => {
                    writer.put_bool(true);
                    writer.put_u32(0);
                    put_id(writer, stateid);
                    writer.put_u32(0);
                    writer.put_u64(0);
                    writer.put_opaque(owner);
                }
                None => {
                    writer.put_bool(false);
                    put_id(writer, stateid);
                    writer.put_u32(0);
                }
            }
        })
    }

    fn lockt(self, offset: u64, owner: &[u8]) -> Self {
        self.op(NfsOp4::Lockt, |writer| {
            writer.put_u32(2);
            writer.put_u64(offset);
            writer.put_u64(1);
            writer.put_u64(0);
            writer.put_opaque(owner);
        })
    }

    fn locku(self, stateid: &Stateid, length: u64) -> Self {
        self.op(NfsOp4::Locku, |writer| {
            writer.put_u32(2);
            writer.put_u32(0);
            put_id(writer, stateid);
            writer.put_u64(0);
            writer.put_u64(length);
        })
    }

    fn commit(self) -> Self {
        self.op(NfsOp4::Commit, |writer| {
            writer.put_u64(0);
            writer.put_u32(0);
        })
    }

    fn test_stateids(self, stateids: &[Stateid]) -> Self {
        self.op(NfsOp4::TestStateid, |writer| {
            writer.put_u32(stateids.len() as u32);
            for stateid in stateids {
                put_id(writer, stateid);
            }
        })
    }

    fn free_stateid(self, stateid: &Stateid) -> Self {
        self.op(NfsOp4::FreeStateid, |writer| put_id(writer, stateid))
    }

    fn rename(self, old: &str, new: &str) -> Self {
        self.op(NfsOp4::Rename, |writer| {
            writer.put_string(old);
            writer.put_string(new);
        })
    }

    fn exchange_id(self, owner: &[u8]) -> Self {
        self.op(NfsOp4::ExchangeId, |writer| {
            writer.put_fixed(&[1; 8]);
            writer.put_opaque(owner);
            writer.put_u32(0);
            writer.put_u32(0);
            writer.put_u32(0);
        })
    }

    fn create_session(self, client_id: u64, sequence: u32, cached: u32) -> Self {
        self.op(NfsOp4::CreateSession, |writer| {
            writer.put_u64(client_id);
            writer.put_u32(sequence);
            writer.put_u32(0);
            put_channel(writer, cached);
            put_channel(writer, cached);
            writer.put_u32(0);
            writer.put_u32(1);
            writer.put_u32(0);
        })
    }

    fn bind(self, session: [u8; 16]) -> Self {
        self.op(NfsOp4::BindConnection, |writer| {
            writer.put_fixed(&session);
            writer.put_u32(3);
            writer.put_bool(false);
        })
    }

    fn body_minor(self, minor: u32) -> Bytes {
        let mut body = XdrWriter::new();
        body.put_opaque(b"");
        body.put_u32(minor);
        body.put_u32(self.count);
        body.put_raw(self.writer.as_slice());
        body.into_bytes()
    }

    fn body(self) -> Bytes {
        self.body_minor(1)
    }
}

fn no_create(writer: &mut XdrWriter) {
    writer.put_u32(0);
}

fn unchecked(writer: &mut XdrWriter) {
    writer.put_u32(1);
    writer.put_u32(0);
    empty_fattr(writer);
}

fn exclusive(verifier: [u8; 8]) -> impl FnOnce(&mut XdrWriter) {
    move |writer: &mut XdrWriter| {
        writer.put_u32(1);
        writer.put_u32(3);
        writer.put_fixed(&verifier);
        empty_fattr(writer);
    }
}

/// A COMPOUND4res read result by result.
struct Reply {
    reader: XdrReader,
    status: u32,
    count: u32,
}

fn reply(outcome: CompoundOutcome) -> Reply {
    let CompoundOutcome::Reply(bytes) = outcome else {
        panic!("expected a compound reply, got {outcome:?}");
    };
    let mut reader = XdrReader::new(bytes);
    let status = reader.u32().expect("status");
    reader.opaque(1024).expect("tag");
    let count = reader.u32().expect("count");
    Reply {
        reader,
        status,
        count,
    }
}

impl Reply {
    /// The status of the next result, after checking its operation.
    fn next(&mut self, code: NfsOp4) -> u32 {
        assert_eq!(self.reader.u32().expect("opcode"), code.code());
        self.reader.u32().expect("result status")
    }

    /// Skips a successful SEQUENCE result and returns its status flags.
    fn sequence(&mut self) -> u32 {
        assert_eq!(self.next(NfsOp4::Sequence), 0);
        self.reader.fixed(16).expect("session");
        for _ in 0..4 {
            self.u32();
        }
        self.u32()
    }

    fn u32(&mut self) -> u32 {
        self.reader.u32().expect("u32")
    }

    fn u64(&mut self) -> u64 {
        self.reader.u64().expect("u64")
    }

    fn flag(&mut self) -> bool {
        self.reader.bool().expect("bool")
    }

    fn opaque(&mut self) -> Bytes {
        self.reader.opaque(1 << 20).expect("opaque")
    }

    fn stateid(&mut self) -> Stateid {
        Stateid {
            seqid: self.u32(),
            other: self.reader.array::<12>().expect("other"),
        }
    }

    /// Reads the OPEN result after its status and returns the stateid.
    fn open_result(&mut self) -> Stateid {
        let stateid = self.stateid();
        self.reader.fixed(20).expect("change info");
        self.u32();
        let words = self.u32();
        for _ in 0..words {
            self.u32();
        }
        assert_eq!(self.u32(), 0, "no delegation");
        stateid
    }

    fn skip_fattr(&mut self) {
        let words = self.u32();
        for _ in 0..words {
            self.u32();
        }
        self.opaque();
    }
}

struct Harness {
    backend: Arc<MockBackend>,
    engine: Engine<MockBackend>,
    fixture: Fixture,
    connection: ConnectionInfo,
    now: Instant,
    session: [u8; 16],
    sequence: u32,
}

impl Harness {
    async fn new() -> Self {
        Self::cached(8192).await
    }

    /// A harness whose session keeps cached replies of at most `cached` bytes.
    async fn cached(cached: u32) -> Self {
        let (backend, fixture) = MockBackend::with_host([1; 32]);
        let engine = Engine::new(
            backend.clone(),
            FilehandleKey::derive(&[3; 32]).expect("key"),
            EngineConfig {
                lease: LEASE,
                limits: ServerLimits {
                    max_request: 1 << 20,
                    max_response: 1 << 20,
                },
                boot: 11,
                session_key: [4; 32],
                server_owner: Bytes::from_static(b"server"),
            },
        );
        let connection = ConnectionInfo {
            id: 1,
            host: fixture.host,
            peer: IpAddr::V4(Ipv4Addr::LOCALHOST),
        };
        let mut harness = Self {
            backend,
            engine,
            fixture,
            connection,
            now: Instant::now(),
            session: [0; 16],
            sequence: 0,
        };
        let mut exchange = harness.run(Ops::new().exchange_id(b"client")).await;
        assert_eq!(exchange.next(NfsOp4::ExchangeId), 0);
        let client_id = exchange.u64();
        let sequence = exchange.u32();
        let mut created = harness
            .run(Ops::new().create_session(client_id, sequence, cached))
            .await;
        assert_eq!(created.next(NfsOp4::CreateSession), 0);
        harness.session = created.reader.array::<16>().expect("session");
        harness
    }

    async fn call(&self, body: Bytes) -> CompoundOutcome {
        self.engine
            .compound(&self.connection, principal(), body, true, self.now)
            .await
    }

    async fn run(&self, ops: Ops) -> Reply {
        reply(self.call(ops.body()).await)
    }

    /// A compound that starts with the next SEQUENCE of slot zero.
    fn ops(&mut self) -> Ops {
        self.sequence += 1;
        Ops::new().sequence(self.session, self.sequence, true)
    }

    fn handle(&self, export: &MountExport, object_id: u64) -> [u8; HANDLE_SIZE] {
        self.engine.key.seal(&Filehandle::object(export, object_id))
    }

    fn root(&self) -> [u8; HANDLE_SIZE] {
        self.handle(&self.fixture.export, ROOT)
    }

    /// Opens `name` under the export root for reading and writing and returns its stateid.
    async fn open(&mut self, name: &str) -> Stateid {
        let ops = self
            .ops()
            .putfh(&self.root())
            .open(b"owner", name, unchecked);
        let mut result = self.run(ops).await;
        assert_eq!(result.status, 0, "open {name}");
        result.sequence();
        assert_eq!(result.next(NfsOp4::Putfh), 0);
        assert_eq!(result.next(NfsOp4::Open), 0);
        result.open_result()
    }
}

// The pseudo-root and `/m` list nothing and grant only traversal; mounts resolve by exact id.
#[tokio::test]
async fn pseudo_root_hidden() {
    let mut harness = Harness::new().await;
    let ops = harness
        .ops()
        .plain(NfsOp4::Putrootfh)
        .readdir(0, [0; 8], 4096)
        .access(access::READ | access::LOOKUP)
        .lookup("m")
        .readdir(0, [0; 8], 4096)
        .access(access::READ | access::LOOKUP);
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putrootfh), 0);
    for lookup in [false, true] {
        if lookup {
            assert_eq!(result.next(NfsOp4::Lookup), 0);
        }
        assert_eq!(result.next(NfsOp4::Readdir), 0);
        result.reader.fixed(8).expect("verifier");
        assert!(!result.flag(), "no entries");
        assert!(result.flag(), "eof");
        assert_eq!(result.next(NfsOp4::Access), 0);
        assert_eq!(result.u32(), access::READ | access::LOOKUP);
        assert_eq!(result.u32(), access::LOOKUP);
    }
    let unknown = Ulid::from_parts(77, 77).to_string();
    let ops = harness
        .ops()
        .plain(NfsOp4::Putrootfh)
        .lookup("m")
        .lookup(&unknown);
    assert_eq!(harness.run(ops).await.status, NfsStat4::NoEntry.code());
    let mount = harness.fixture.export.mount_id.to_string();
    let ops = harness
        .ops()
        .plain(NfsOp4::Putrootfh)
        .lookup("m")
        .lookup(&mount)
        .plain(NfsOp4::Getfh);
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    for code in [NfsOp4::Putrootfh, NfsOp4::Lookup, NfsOp4::Lookup] {
        assert_eq!(result.next(code), 0);
    }
    assert_eq!(result.next(NfsOp4::Getfh), 0);
    assert_eq!(&result.opaque()[..], &harness.root()[..]);
}

// A retry replays the cached bytes without running the WRITE again; a changed body on the
// same slot sequence is a false retry, and a gap is misordered.
#[tokio::test]
async fn slot_replay_once() {
    let mut harness = Harness::new().await;
    let stateid = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let body = harness
        .ops()
        .putfh(&handle)
        .write(&stateid, 0, b"xy")
        .body();
    let first = harness.call(body.clone()).await;
    assert_eq!(harness.backend.state.lock().writes, 1);
    let again = harness.call(body).await;
    assert_eq!(first, again, "a retry returns the cached reply");
    assert_eq!(harness.backend.state.lock().writes, 1, "the WRITE ran once");
    let changed = Ops::new()
        .sequence(harness.session, harness.sequence, true)
        .putfh(&handle)
        .write(&stateid, 0, b"zz");
    assert_eq!(
        harness.run(changed).await.status,
        NfsStat4::FalseRetry.code()
    );
    let gap = Ops::new().sequence(harness.session, harness.sequence + 2, true);
    assert_eq!(
        harness.run(gap).await.status,
        NfsStat4::SequenceMisordered.code()
    );
    harness.sequence += 1;
    let uncached = Ops::new()
        .sequence(harness.session, harness.sequence, false)
        .putfh(&handle)
        .write(&stateid, 2, b"ab")
        .body();
    assert_eq!(reply(harness.call(uncached.clone()).await).status, 0);
    let retried = reply(harness.call(uncached).await);
    assert_eq!(retried.status, NfsStat4::RetryUncached.code());
    assert_eq!(harness.backend.state.lock().writes, 2);
}

// A cached reply whose grant no longer holds is refused at RPC admission; the slot keeps
// its reply and the compound never runs again.
#[tokio::test]
async fn replay_refused_unchanged() {
    let mut harness = Harness::new().await;
    let stateid = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let body = harness
        .ops()
        .putfh(&handle)
        .write(&stateid, 0, b"xy")
        .body();
    let first = harness.call(body.clone()).await;
    harness.backend.state.lock().replay_denied = true;
    assert_eq!(
        harness.call(body.clone()).await,
        CompoundOutcome::Denied(AuthStat::TooWeak)
    );
    assert_eq!(harness.backend.state.lock().writes, 1);
    harness.backend.state.lock().replay_denied = false;
    assert_eq!(
        harness.call(body).await,
        first,
        "the slot still holds its reply"
    );
    let other = harness
        .engine
        .compound(
            &harness.connection,
            RpcPrincipal::Sys { uid: 0, gid: 0 },
            Ops::new()
                .sequence(harness.session, harness.sequence, true)
                .putfh(&handle)
                .write(&stateid, 0, b"xy")
                .body(),
            true,
            harness.now,
        )
        .await;
    assert_eq!(
        other,
        CompoundOutcome::Denied(AuthStat::TooWeak),
        "another RPC principal"
    );
    assert_eq!(harness.backend.state.lock().writes, 1);
}

// After a restart no state survives, so every reclaim is NO_GRACE while fresh opens work.
#[tokio::test]
async fn reclaims_refused() {
    let mut harness = Harness::new().await;
    let ops = harness.ops().putfh(&harness.root()).open_previous(b"owner");
    assert_eq!(harness.run(ops).await.status, NfsStat4::NoGrace.code());
    let stateid = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let ops = harness
        .ops()
        .putfh(&handle)
        .lock(true, 10, &stateid, Some(b"locker"));
    assert_eq!(harness.run(ops).await.status, NfsStat4::NoGrace.code());
}

// PUTFH installs an expired volatile handle and the first operation using it reports
// FHEXPIRED; a forged handle is BADHANDLE and a deleted one STALE at PUTFH.
#[tokio::test]
async fn expired_handle_deferred() {
    let mut harness = Harness::new().await;
    let file = harness
        .backend
        .add_file(harness.fixture.export.mount_id, "data", b"abc");
    harness.backend.state.lock().expired.insert(file);
    let handle = harness.handle(&harness.fixture.export, file);
    let ops = harness.ops().putfh(&handle).getattr(&[attribute::TYPE]);
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, NfsStat4::FilehandleExpired.code());
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(
        result.next(NfsOp4::Getattr),
        NfsStat4::FilehandleExpired.code()
    );
    let ops = harness
        .ops()
        .putfh(&handle)
        .plain(NfsOp4::Putrootfh)
        .getattr(&[attribute::TYPE]);
    assert_eq!(
        harness.run(ops).await.status,
        0,
        "a replaced handle never reports"
    );
    let mut forged = handle;
    forged[30] ^= 1;
    let ops = harness.ops().putfh(&forged);
    assert_eq!(harness.run(ops).await.status, NfsStat4::BadHandle.code());
    harness.backend.state.lock().stale.insert(file);
    let ops = harness.ops().putfh(&handle);
    assert_eq!(harness.run(ops).await.status, NfsStat4::Stale.code());
}

// The sweeper revokes an expired lease without any request; SEQUENCE then reports it and
// TEST_STATEID keeps EXPIRED until FREE_STATEID.
#[tokio::test]
async fn lease_expiry_reported() {
    let mut harness = Harness::new().await;
    let stateid = harness.open("data").await;
    harness.now += LEASE + Duration::from_secs(1);
    harness.engine.sweep(harness.now).await;
    assert_eq!(
        harness.backend.state.lock().closes,
        1,
        "the expired open released"
    );
    let ops = harness.ops().test_stateids(&[stateid]);
    let mut result = harness.run(ops).await;
    assert_eq!(result.sequence(), sequence_flag::EXPIRED_ALL);
    assert_eq!(result.next(NfsOp4::TestStateid), 0);
    assert_eq!(result.u32(), 1);
    assert_eq!(result.u32(), NfsStat4::Expired.code());
    let ops = harness
        .ops()
        .free_stateid(&stateid)
        .test_stateids(&[stateid]);
    let mut result = harness.run(ops).await;
    assert_eq!(result.sequence(), sequence_flag::EXPIRED_ALL);
    assert_eq!(result.next(NfsOp4::FreeStateid), 0);
    assert_eq!(result.next(NfsOp4::TestStateid), 0);
    assert_eq!(result.u32(), 1);
    assert_eq!(result.u32(), NfsStat4::BadStateid.code());
    let ops = harness.ops();
    assert_eq!(harness.run(ops).await.sequence(), 0);
}

// Deleting one binding revokes only its state, reported as admin revocation.
#[tokio::test]
async fn binding_revocation_scoped() {
    let mut harness = Harness::new().await;
    let first = harness.open("data").await;
    let other_root = harness.handle(&harness.fixture.other, ROOT);
    let ops = harness
        .ops()
        .putfh(&other_root)
        .open(b"owner", "data", unchecked);
    let mut result = harness.run(ops).await;
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Open), 0);
    let second = result.open_result();
    let revocation = Revocation::Mount(harness.fixture.export.mount_id);
    harness.engine.revoke(&revocation).await;
    let ops = harness.ops().test_stateids(&[first, second]);
    let mut result = harness.run(ops).await;
    assert_eq!(result.sequence(), sequence_flag::ADMIN_REVOKED);
    assert_eq!(result.next(NfsOp4::TestStateid), 0);
    assert_eq!(result.u32(), 2);
    assert_eq!(result.u32(), NfsStat4::AdminRevoked.code());
    assert_eq!(result.u32(), NfsStat4::Ok.code());
}

// The current stateid follows OPEN, survives SAVEFH and RESTOREFH and is cleared by PUTFH;
// anonymous and bypass stateids read without an open.
#[tokio::test]
async fn current_stateid_rules() {
    let mut harness = Harness::new().await;
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .open(b"owner", "data", unchecked)
        .write(&Stateid::CURRENT, 0, b"ab")
        .plain(NfsOp4::Savefh)
        .plain(NfsOp4::Putrootfh)
        .plain(NfsOp4::Restorefh)
        .write(&Stateid::CURRENT, 2, b"cd");
    assert_eq!(harness.run(ops).await.status, 0);
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .open(b"owner", "data", no_create)
        .putfh(&handle)
        .write(&Stateid::CURRENT, 0, b"ef");
    assert_eq!(harness.run(ops).await.status, NfsStat4::BadStateid.code());
    for stateid in [Stateid::ANONYMOUS, Stateid::BYPASS] {
        let ops = harness.ops().putfh(&handle).read(&stateid, 16);
        let mut result = harness.run(ops).await;
        assert_eq!(result.status, 0);
        result.sequence();
        assert_eq!(result.next(NfsOp4::Putfh), 0);
        assert_eq!(result.next(NfsOp4::Read), 0);
        assert!(result.flag(), "eof");
        assert_eq!(&result.opaque()[..], b"abcd");
    }
    let ops = harness
        .ops()
        .putfh(&handle)
        .write(&Stateid::INVALID, 0, b"x");
    assert_eq!(harness.run(ops).await.status, NfsStat4::BadStateid.code());
}

// LOCKT and COMMIT carry no stateid; CLOSE waits for LOCKU and returns the invalid stateid.
#[tokio::test]
async fn locks_and_commit() {
    let mut harness = Harness::new().await;
    let open = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let ops = harness
        .ops()
        .putfh(&handle)
        .lock(false, 10, &open, Some(b"lock-a"));
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Lock), 0);
    let lock = result.stateid();
    assert_eq!(lock.seqid, 1);
    let ops = harness.ops().putfh(&handle).lockt(5, b"lock-b").commit();
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, NfsStat4::Denied.code());
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Lockt), NfsStat4::Denied.code());
    assert_eq!((result.u64(), result.u64(), result.u32()), (0, 10, 2));
    result.u64();
    assert_eq!(&result.opaque()[..], b"lock-a");
    let ops = harness.ops().putfh(&handle).commit();
    let mut result = harness.run(ops).await;
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Commit), 0);
    assert_eq!(
        result.reader.array::<8>().expect("verifier"),
        WRITE_VERIFIER
    );
    let ops = harness.ops().putfh(&handle).close(&open);
    assert_eq!(harness.run(ops).await.status, NfsStat4::LocksHeld.code());
    let ops = harness.ops().putfh(&handle).locku(&lock, 10);
    assert_eq!(harness.run(ops).await.status, 0);
    let ops = harness.ops().putfh(&handle).locku(&lock, 10);
    assert_eq!(harness.run(ops).await.status, NfsStat4::OldStateid.code());
    let ops = harness.ops().putfh(&handle).close(&open);
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Close), 0);
    assert_eq!(result.stateid(), Stateid::INVALID);
}

// A retried EXCLUSIVE4_1 create finds its own file; another verifier is EXIST.
#[tokio::test]
async fn exclusive_create_replays() {
    let mut harness = Harness::new().await;
    for _ in 0..2 {
        let ops = harness
            .ops()
            .putfh(&harness.root())
            .open(b"owner", "new", exclusive([1; 8]));
        assert_eq!(harness.run(ops).await.status, 0);
    }
    assert_eq!(harness.backend.state.lock().creates, 1);
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .open(b"other", "new", exclusive([2; 8]));
    assert_eq!(harness.run(ops).await.status, NfsStat4::Exist.code());
}

#[tokio::test]
async fn rename_across_exports() {
    let mut harness = Harness::new().await;
    let other_root = harness.handle(&harness.fixture.other, ROOT);
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .plain(NfsOp4::Savefh)
        .putfh(&other_root)
        .rename("a", "b");
    assert_eq!(harness.run(ops).await.status, NfsStat4::Xdev.code());
}

// SEQUENCE comes first, session-less operations come alone, and minor version 1 only.
#[tokio::test]
async fn position_rules_enforced() {
    let mut harness = Harness::new().await;
    let lone = Ops::new().plain(NfsOp4::Putrootfh);
    assert_eq!(
        harness.run(lone).await.status,
        NfsStat4::NotInSession.code()
    );
    let paired = Ops::new().exchange_id(b"client").plain(NfsOp4::Putrootfh);
    assert_eq!(
        harness.run(paired).await.status,
        NfsStat4::NotOnlyOperation.code()
    );
    let minor = reply(
        harness
            .call(Ops::new().plain(NfsOp4::Putrootfh).body_minor(0))
            .await,
    );
    assert_eq!(
        (minor.status, minor.count),
        (NfsStat4::MinorMismatch.code(), 0)
    );
    let session = harness.session;
    let twice = harness.ops().sequence(session, 99, false);
    assert_eq!(
        harness.run(twice).await.status,
        NfsStat4::SequencePosition.code()
    );
    let illegal = harness
        .ops()
        .plain(NfsOp4::Putrootfh)
        .op(NfsOp4::Illegal, |_| {});
    let mut result = harness.run(illegal).await;
    assert_eq!(result.status, NfsStat4::OperationIllegal.code());
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putrootfh), 0);
    assert_eq!(
        result.next(NfsOp4::Illegal),
        NfsStat4::OperationIllegal.code()
    );
}

// A connection of another host never binds to the session; one of the same host does.
#[tokio::test]
async fn bind_same_host() {
    let mut harness = Harness::new().await;
    let stranger = ConnectionInfo {
        id: 2,
        host: HostIdentity {
            host_id: Ulid::from_parts(8, 8),
            owner: harness.fixture.host.owner,
            fingerprint: None,
        },
        peer: harness.connection.peer,
    };
    let body = Ops::new().bind(harness.session).body();
    let refused = harness
        .engine
        .compound(&stranger, principal(), body.clone(), true, harness.now)
        .await;
    assert_eq!(reply(refused).status, NfsStat4::BadSession.code());
    harness.connection.id = 3;
    assert_eq!(reply(harness.call(body).await).status, 0);
}

// READDIR reserves cookies 1 and 2, honors maxcount and resumes from an entry cookie.
#[tokio::test]
async fn readdir_cookies_resume() {
    let mut harness = Harness::new().await;
    let mount = harness.fixture.export.mount_id;
    harness.backend.add_file(mount, "a", b"1");
    harness.backend.add_file(mount, "b", b"2");
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .readdir(1, [0; 8], 4096);
    assert_eq!(harness.run(ops).await.status, NfsStat4::BadCookie.code());
    let ops = harness.ops().putfh(&harness.root()).readdir(0, [0; 8], 8);
    assert_eq!(harness.run(ops).await.status, NfsStat4::TooSmall.code());
    let ops = harness.ops().putfh(&harness.root()).readdir(0, [0; 8], 24);
    assert_eq!(harness.run(ops).await.status, NfsStat4::TooSmall.code());
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .readdir(0, [0; 8], 4096);
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Readdir), 0);
    assert_eq!(
        result.reader.array::<8>().expect("verifier"),
        DIRECTORY_VERIFIER
    );
    let mut cookies = Vec::new();
    while result.flag() {
        cookies.push(result.u64());
        result.opaque();
        result.skip_fattr();
    }
    assert!(result.flag(), "eof");
    assert_eq!(cookies.len(), 2);
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .readdir(cookies[0], DIRECTORY_VERIFIER, 4096);
    let mut result = harness.run(ops).await;
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Readdir), 0);
    result.reader.fixed(8).expect("verifier");
    assert!(result.flag());
    assert_eq!(result.u64(), cookies[1]);
    assert_eq!(&result.opaque()[..], b"b");
    let ops = harness
        .ops()
        .putfh(&harness.root())
        .readdir(cookies[0], [1; 8], 4096);
    assert_eq!(harness.run(ops).await.status, NfsStat4::NotSame.code());
}

// A cached compound stops at the first result its slot cannot keep with REP_TOO_BIG_TO_CACHE,
// and a retry replays exactly that prefix instead of running anything again.
#[tokio::test]
async fn cached_reply_bounded() {
    let mut harness = Harness::cached(512).await;
    let mut ops = harness.ops().putfh(&harness.root());
    for _ in 0..12 {
        ops = ops.plain(NfsOp4::Getfh);
    }
    let body = ops.body();
    let first = harness.call(body.clone()).await;
    let CompoundOutcome::Reply(bytes) = &first else {
        panic!("expected a reply, got {first:?}");
    };
    assert!(bytes.len() <= 512, "the reply fits the cached size");
    let mut result = reply(first.clone());
    assert_eq!(result.status, NfsStat4::ReplyUncacheable.code());
    assert!(result.count < 14, "the compound stopped early");
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    for _ in 2..result.count - 1 {
        assert_eq!(result.next(NfsOp4::Getfh), 0);
        result.opaque();
    }
    let last = result.next(NfsOp4::Getfh);
    assert_eq!(last, NfsStat4::ReplyUncacheable.code());
    assert_eq!(harness.call(body).await, first, "the slot kept the reply");
}

// A large READ in a cached compound is shortened to what the slot can keep.
#[tokio::test]
async fn cached_read_shortened() {
    let mut harness = Harness::cached(1024).await;
    let mount = harness.fixture.export.mount_id;
    let file = harness.backend.add_file(mount, "big", &[7; 4096]);
    let handle = harness.handle(&harness.fixture.export, file);
    let body = harness
        .ops()
        .putfh(&handle)
        .read(&Stateid::ANONYMOUS, 4096)
        .body();
    let first = harness.call(body.clone()).await;
    let mut result = reply(first.clone());
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Read), 0);
    assert!(!result.flag(), "a short read is no eof");
    let data = result.opaque();
    assert!(!data.is_empty() && data.len() < 1024);
    assert_eq!(harness.call(body).await, first, "the slot kept the reply");
}

// A retry of an uncached reply succeeds its SEQUENCE and fails the next operation with
// RETRY_UNCACHED_REP (RFC 8881 section 2.10.6.1.3).
#[tokio::test]
async fn uncached_retry_placed() {
    let mut harness = Harness::new().await;
    harness.sequence += 1;
    let body = Ops::new()
        .sequence(harness.session, harness.sequence, false)
        .putfh(&harness.root())
        .plain(NfsOp4::Getfh)
        .body();
    assert_eq!(reply(harness.call(body.clone()).await).status, 0);
    let mut retried = reply(harness.call(body).await);
    assert_eq!(retried.status, NfsStat4::RetryUncached.code());
    assert_eq!(retried.count, 2);
    retried.sequence();
    assert_eq!(retried.next(NfsOp4::Putfh), NfsStat4::RetryUncached.code());
}

// A cached reply records each object use, so its replay is admitted for exactly those; a READ
// keeps the exact source the backend served, not just its range.
#[tokio::test]
async fn replay_records_uses() {
    let mut harness = Harness::new().await;
    let stateid = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let object = ObjectRef {
        mount_id: harness.fixture.export.mount_id,
        object_id: file,
    };
    let handle = harness.handle(&harness.fixture.export, file);
    let body = harness
        .ops()
        .putfh(&handle)
        .write(&stateid, 0, b"xy")
        .read(&stateid, 16)
        .body();
    let first = harness.call(body.clone()).await;
    assert_eq!(harness.call(body).await, first);
    let grant = harness.backend.state.lock().replayed.clone();
    let checks = grant.expect("the replay was checked").checks;
    let source = crate::backend::ReadSource {
        key: "data".to_string(),
        version_id: None,
        instance_id: Some(file),
    };
    let read = ReplayAccess::Read {
        offset: 0,
        length: 16,
        source: Some(source),
    };
    for access in [ReplayAccess::Write, read] {
        let check = ReplayCheck {
            object,
            access: access.clone(),
        };
        assert!(checks.contains(&check), "{access:?}");
    }
}

// OPEN with create hands its deny bits to the creating backend open of the same owner.
#[tokio::test]
async fn create_keeps_deny() {
    let mut harness = Harness::new().await;
    let ops =
        harness
            .ops()
            .putfh(&harness.root())
            .open_deny(b"owner", ("new", share::WRITE), unchecked);
    assert_eq!(harness.run(ops).await.status, 0);
    let created = harness.backend.state.lock().created.clone();
    let share = created.last().expect("a create");
    assert_eq!((share.access, share.deny), (share::BOTH, share::WRITE));
    assert_eq!(&share.owner.owner[..], b"owner");
}

// A WRITE with a lock stateid runs as that lock owner, not as the open owner behind it.
#[tokio::test]
async fn lock_owner_writes() {
    let mut harness = Harness::new().await;
    let open = harness.open("data").await;
    let file = harness.backend.state.lock().next_object - 1;
    let handle = harness.handle(&harness.fixture.export, file);
    let ops = harness
        .ops()
        .putfh(&handle)
        .lock(false, 10, &open, Some(b"lock-a"));
    let mut result = harness.run(ops).await;
    assert_eq!(result.status, 0);
    result.sequence();
    assert_eq!(result.next(NfsOp4::Putfh), 0);
    assert_eq!(result.next(NfsOp4::Lock), 0);
    let lock = result.stateid();
    let ops = harness.ops().putfh(&handle).write(&lock, 0, b"xy");
    assert_eq!(harness.run(ops).await.status, 0);
    let ops = harness.ops().putfh(&handle).write(&open, 2, b"zz");
    assert_eq!(harness.run(ops).await.status, 0);
    let writers = harness.backend.state.lock().writers.clone();
    let owners: Vec<&[u8]> = writers.iter().map(|owner| &owner.owner[..]).collect();
    assert_eq!(owners, [&b"lock-a"[..], &b"owner"[..]]);
}

// A release of revoked state that fails stays pending, and the next sweep retries it until
// the backend has ended the open.
#[tokio::test]
async fn failed_release_retried() {
    let mut harness = Harness::new().await;
    harness.open("data").await;
    let closes = {
        let mut state = harness.backend.state.lock();
        state.close_failures = 1;
        state.closes
    };
    let revocation = Revocation::Mount(harness.fixture.export.mount_id);
    harness.engine.revoke(&revocation).await;
    assert_eq!(harness.backend.state.lock().closes, closes + 1);
    assert_eq!(
        harness.engine.pending.lock().len(),
        1,
        "the failed close stays"
    );
    harness.engine.sweep(harness.now).await;
    assert_eq!(harness.backend.state.lock().closes, closes + 2);
    assert!(
        harness.engine.pending.lock().is_empty(),
        "the retry released it"
    );
    harness.engine.sweep(harness.now).await;
    assert_eq!(harness.backend.state.lock().closes, closes + 2);
}

// A cached READ either returns some bytes or stops the compound with REP_TOO_BIG_TO_CACHE once
// no data byte fits; it never answers zero bytes short of the end.
#[tokio::test]
async fn cached_read_progresses() {
    let limit = 200;
    let mut harness = Harness::cached(limit).await;
    let mount = harness.fixture.export.mount_id;
    let file = harness.backend.add_file(mount, "big", &[7; 4096]);
    let handle = harness.handle(&harness.fixture.export, file);
    let mut refused_read = false;
    for fillers in 0..12 {
        let mut ops = harness.ops().putfh(&handle);
        for _ in 0..fillers {
            ops = ops.getattr(&[]);
        }
        let outcome = harness
            .call(ops.read(&Stateid::ANONYMOUS, 4096).body())
            .await;
        let CompoundOutcome::Reply(bytes) = &outcome else {
            panic!("expected a reply, got {outcome:?}");
        };
        assert!(
            bytes.len() <= limit as usize,
            "the reply fits the cached size"
        );
        let mut result = reply(outcome);
        result.sequence();
        assert_eq!(result.next(NfsOp4::Putfh), 0);
        let mut status = 0;
        for _ in 0..fillers {
            status = result.next(NfsOp4::Getattr);
            if status != 0 {
                break;
            }
            result.skip_fattr();
        }
        if status != 0 {
            assert_eq!(status, NfsStat4::ReplyUncacheable.code());
            continue;
        }
        let read = result.next(NfsOp4::Read);
        if read == NfsStat4::ReplyUncacheable.code() {
            refused_read = true;
            continue;
        }
        assert_eq!(read, 0);
        assert!(!result.flag(), "a short read is no eof");
        assert!(!result.opaque().is_empty(), "a read makes progress");
    }
    assert!(refused_read, "a READ with no room left is refused");
}
