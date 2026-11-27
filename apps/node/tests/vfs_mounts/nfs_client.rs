//! An NFSv4.1 client over loopback TCP or RFC 9289 TLS: AUTH_SYS calls, one session slot and
//! COMPOUNDs built on the public XDR, record and type modules of `aruna-nfs`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aruna_nfs::record::{read_record, write_record};
use aruna_nfs::rpc::{COMPOUND_PROCEDURE, NFS_PROGRAM, NFS_VERSION, NULL_PROCEDURE};
use aruna_nfs::rpc::{RPC_VERSION, STARTTLS, flavor};
use aruna_nfs::tls::SUNRPC_ALPN;
use aruna_nfs::types::{NfsOp4, Stateid, share};
use aruna_nfs::xdr::{XdrReader, XdrWriter};
use bytes::Bytes;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout as time_limit;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use super::HostKey;
use crate::shared::TestResult;

/// Deadlock cap of one network step; an honest step takes far less.
const STEP: Duration = Duration::from_secs(120);
const RECORD_LIMIT: usize = 4 << 20;
const BYTES_LIMIT: usize = 2 << 20;
/// The name the test server certificate is issued for.
pub(crate) const SERVER_NAME: &str = "nfs.test";
pub(crate) const UNSTABLE: u32 = 0;
pub(crate) const FILE_SYNC: u32 = 2;
const DIRECTORY_TYPE: u32 = 2;

/// How an OPEN asks for its file: the share access and deny bits and whether it creates.
#[derive(Clone, Copy)]
pub(crate) struct Share {
    pub(crate) access: u32,
    pub(crate) deny: u32,
    pub(crate) create: bool,
}

pub(crate) const READER: Share = Share {
    access: share::READ,
    deny: 0,
    create: false,
};
pub(crate) const CREATOR: Share = Share {
    access: share::BOTH,
    deny: 0,
    create: true,
};

/// One RPC call record with AUTH_SYS, or the AUTH_TLS credential of the STARTTLS probe.
fn call_record(xid: u32, procedure: u32, probe: bool, body: &[u8]) -> Bytes {
    let mut writer = XdrWriter::new();
    for value in [xid, 0, RPC_VERSION, NFS_PROGRAM, NFS_VERSION, procedure] {
        writer.put_u32(value);
    }
    if probe {
        writer.put_u32(flavor::TLS);
        writer.put_opaque(&[]);
    } else {
        let mut system = XdrWriter::new();
        system.put_u32(7);
        system.put_string("aruna-test");
        for value in [1000, 1000, 0] {
            system.put_u32(value);
        }
        writer.put_u32(flavor::SYS);
        writer.put_opaque(system.as_slice());
    }
    writer.put_u32(flavor::NONE);
    writer.put_opaque(&[]);
    writer.put_raw(body);
    writer.into_bytes()
}

/// The accepted reply of `xid`: its verifier and the procedure result after it.
fn accepted(record: Bytes, xid: u32) -> TestResult<(Bytes, XdrReader)> {
    let mut reader = XdrReader::new(record);
    let header = (reader.u32()?, reader.u32()?, reader.u32()?);
    if header != (xid, 1, 0) {
        return Err(format!("RPC reply {header:?} is no accepted reply to {xid}").into());
    }
    reader.u32()?;
    let verifier = reader.opaque(400)?;
    match reader.u32()? {
        0 => Ok((verifier, reader)),
        stat => Err(format!("RPC call {xid} was not run: accept status {stat}").into()),
    }
}

fn put_stateid(writer: &mut XdrWriter, stateid: &Stateid) {
    writer.put_u32(stateid.seqid);
    writer.put_fixed(&stateid.other);
}

/// The operations of one COMPOUND under construction.
pub(crate) struct Compound {
    ops: XdrWriter,
    count: u32,
}

impl Compound {
    fn new() -> Self {
        Self {
            ops: XdrWriter::new(),
            count: 0,
        }
    }

    fn op(mut self, code: NfsOp4, arguments: impl FnOnce(&mut XdrWriter)) -> Self {
        self.ops.put_u32(code.code());
        arguments(&mut self.ops);
        self.count += 1;
        self
    }

    fn putfh(self, handle: &[u8]) -> Self {
        self.op(NfsOp4::Putfh, |ops| ops.put_opaque(handle))
    }

    fn lookup(self, name: &str) -> Self {
        self.op(NfsOp4::Lookup, |ops| ops.put_string(name))
    }

    fn getfh(self) -> Self {
        self.op(NfsOp4::Getfh, |_| {})
    }

    fn body(self) -> Vec<u8> {
        let mut body = XdrWriter::new();
        body.put_opaque(b"");
        body.put_u32(1);
        body.put_u32(self.count);
        body.put_raw(self.ops.as_slice());
        body.as_slice().to_vec()
    }
}

/// The results of one COMPOUND, read operation by operation.
pub(crate) struct Results {
    reader: XdrReader,
}

impl Results {
    /// Expects the next result to be a success of `code`.
    fn next(&mut self, code: NfsOp4) -> TestResult<()> {
        let (found, stat) = (self.reader.u32()?, self.reader.u32()?);
        if found != code.code() || stat != 0 {
            return Err(format!("NFS {code:?} answered operation {found} status {stat}").into());
        }
        Ok(())
    }

    fn skip_bitmap(&mut self) -> TestResult<()> {
        for _ in 0..self.reader.u32()? {
            self.reader.u32()?;
        }
        Ok(())
    }

    fn stateid(&mut self) -> TestResult<Stateid> {
        Ok(Stateid {
            seqid: self.reader.u32()?,
            other: self.reader.array::<12>()?,
        })
    }

    fn handle(&mut self) -> TestResult<Vec<u8>> {
        self.next(NfsOp4::Getfh)?;
        Ok(self.reader.opaque(128)?.to_vec())
    }
}

/// An NFSv4.1 client with one session and slot zero, on a plain or TLS stream.
pub(crate) struct NfsClient<S> {
    stream: S,
    xid: u32,
    session: [u8; 16],
    sequence: u32,
}

/// A TCP connection to a loopback-cleartext server.
pub(crate) async fn plain(address: SocketAddr) -> TestResult<NfsClient<TcpStream>> {
    NfsClient::start(TcpStream::connect(address).await?).await
}

/// The client configuration of a host key that trusts `authority` and speaks `sunrpc`.
fn tls_config(authority: &CertificateDer<'static>, host: &HostKey) -> TestResult<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(authority.clone())?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(host.key.serialize_der()));
    let certificate = host.certificate.der().clone();
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![certificate], key)?;
    config.alpn_protocols = vec![SUNRPC_ALPN.to_vec()];
    Ok(config)
}

/// Sends the AUTH_TLS probe, expects STARTTLS and upgrades to TLS 1.3 with the host key.
pub(crate) async fn secure(
    address: SocketAddr,
    authority: &CertificateDer<'static>,
    host: &HostKey,
) -> TestResult<TlsStream<TcpStream>> {
    let mut tcp = TcpStream::connect(address).await?;
    write_record(&mut tcp, &call_record(1, NULL_PROCEDURE, true, &[])).await?;
    let record = time_limit(STEP, read_record(&mut tcp, RECORD_LIMIT)).await??;
    let (verifier, _) = accepted(record.ok_or("no STARTTLS answer")?, 1)?;
    assert_eq!(&verifier[..], STARTTLS, "the server offers STARTTLS");
    let connector = TlsConnector::from(Arc::new(tls_config(authority, host)?));
    let name = ServerName::try_from(SERVER_NAME).map_err(|_| "invalid server name")?;
    Ok(time_limit(STEP, connector.connect(name, tcp)).await??)
}

impl<S: AsyncRead + AsyncWrite + Unpin> NfsClient<S> {
    /// Runs EXCHANGE_ID and CREATE_SESSION on `stream`.
    pub(crate) async fn start(stream: S) -> TestResult<Self> {
        let mut client = Self {
            stream,
            xid: 100,
            session: [0; 16],
            sequence: 0,
        };
        let owner = ulid::Ulid::generate().to_string();
        let exchange = Compound::new().op(NfsOp4::ExchangeId, |ops| {
            ops.put_fixed(&[3; 8]);
            ops.put_opaque(owner.as_bytes());
            for value in [0, 0, 0] {
                ops.put_u32(value);
            }
        });
        let mut results = client.run(exchange).await?;
        results.next(NfsOp4::ExchangeId)?;
        let (client_id, sequence) = (results.reader.u64()?, results.reader.u32()?);
        let create = Compound::new().op(NfsOp4::CreateSession, |ops| {
            ops.put_u64(client_id);
            ops.put_u32(sequence);
            ops.put_u32(0);
            for _channel in 0..2 {
                for value in [0, 1 << 21, 1 << 21, 8192, 16, 4, 0] {
                    ops.put_u32(value);
                }
            }
            for value in [0, 1, 0] {
                ops.put_u32(value);
            }
        });
        let mut results = client.run(create).await?;
        results.next(NfsOp4::CreateSession)?;
        client.session = results.reader.array::<16>()?;
        Ok(client)
    }

    /// Sends one COMPOUND and returns its results; a failed operation fails the call.
    async fn run(&mut self, compound: Compound) -> TestResult<Results> {
        self.xid += 1;
        let xid = self.xid;
        let record = call_record(xid, COMPOUND_PROCEDURE, false, &compound.body());
        write_record(&mut self.stream, &record).await?;
        let reply = time_limit(STEP, read_record(&mut self.stream, RECORD_LIMIT)).await??;
        let (_, mut reader) = accepted(reply.ok_or("the NFS server closed the connection")?, xid)?;
        let status = reader.u32()?;
        reader.opaque(1024)?;
        reader.u32()?;
        let results = Results { reader };
        match status {
            0 => Ok(results),
            _ => Err(format!("NFS COMPOUND {xid} failed with status {status}").into()),
        }
    }

    /// A COMPOUND that starts with the next SEQUENCE of slot zero.
    fn sequenced(&mut self) -> Compound {
        self.sequence += 1;
        let (session, sequence) = (self.session, self.sequence);
        Compound::new().op(NfsOp4::Sequence, move |ops| {
            ops.put_fixed(&session);
            for value in [sequence, 0, 0] {
                ops.put_u32(value);
            }
            ops.put_bool(false);
        })
    }

    async fn run_sequenced(&mut self, compound: Compound) -> TestResult<Results> {
        let mut results = self.run(compound).await?;
        results.next(NfsOp4::Sequence)?;
        results.reader.fixed(16)?;
        for _ in 0..5 {
            results.reader.u32()?;
        }
        Ok(results)
    }

    /// The root handle of the binding `mount_id` under the pseudo-root `/m`.
    pub(crate) async fn export_root(&mut self, mount_id: &str) -> TestResult<Vec<u8>> {
        let compound = self.sequenced().op(NfsOp4::Putrootfh, |_| {});
        let compound = compound.lookup("m").lookup(mount_id).getfh();
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putrootfh)?;
        results.next(NfsOp4::Lookup)?;
        results.next(NfsOp4::Lookup)?;
        results.handle()
    }

    /// Looks up one child handle under `parent`.
    pub(crate) async fn lookup(&mut self, parent: &[u8], name: &str) -> TestResult<Vec<u8>> {
        let compound = self.sequenced().putfh(parent).lookup(name).getfh();
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Lookup)?;
        results.handle()
    }

    pub(crate) async fn mkdir(&mut self, parent: &[u8], name: &str) -> TestResult<Vec<u8>> {
        let compound = self.sequenced().putfh(parent);
        let compound = compound.op(NfsOp4::Create, |ops| {
            ops.put_u32(DIRECTORY_TYPE);
            ops.put_string(name);
            ops.put_u32(0);
            ops.put_u32(0);
        });
        let mut results = self.run_sequenced(compound.getfh()).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Create)?;
        results.reader.fixed(20)?;
        results.skip_bitmap()?;
        results.handle()
    }

    /// Opens `name` in `parent` and returns the open stateid and the file handle.
    pub(crate) async fn open(
        &mut self,
        parent: &[u8],
        name: &str,
        mode: Share,
    ) -> TestResult<(Stateid, Vec<u8>)> {
        let owner = ulid::Ulid::generate().to_bytes();
        let compound = self.sequenced().putfh(parent);
        let compound = compound.op(NfsOp4::Open, |ops| {
            for value in [0, mode.access, mode.deny] {
                ops.put_u32(value);
            }
            ops.put_u64(0);
            ops.put_opaque(&owner);
            // OPEN4_CREATE with UNCHECKED4 and no attributes, or OPEN4_NOCREATE.
            let create: &[u32] = if mode.create { &[1, 0, 0, 0] } else { &[0] };
            for value in create {
                ops.put_u32(*value);
            }
            ops.put_u32(0);
            ops.put_string(name);
        });
        let mut results = self.run_sequenced(compound.getfh()).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Open)?;
        let stateid = results.stateid()?;
        results.reader.fixed(20)?;
        results.reader.u32()?;
        results.skip_bitmap()?;
        let delegation = results.reader.u32()?;
        assert_eq!(delegation, 0, "OPEN of {name} grants no delegation");
        Ok((stateid, results.handle()?))
    }

    pub(crate) async fn write(
        &mut self,
        file: &[u8],
        stateid: &Stateid,
        data: &[u8],
        stable: u32,
    ) -> TestResult<()> {
        let compound = self.sequenced().putfh(file);
        let compound = compound.op(NfsOp4::Write, |ops| {
            put_stateid(ops, stateid);
            ops.put_u64(0);
            ops.put_u32(stable);
            ops.put_opaque(data);
        });
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Write)?;
        assert_eq!(
            results.reader.u32()? as usize,
            data.len(),
            "WRITE takes every byte"
        );
        Ok(())
    }

    pub(crate) async fn commit(&mut self, file: &[u8]) -> TestResult<()> {
        let compound = self.sequenced().putfh(file).op(NfsOp4::Commit, |ops| {
            ops.put_u64(0);
            ops.put_u32(0);
        });
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Commit)
    }

    /// Reads the whole file from offset zero in READ calls until end of file.
    pub(crate) async fn read(&mut self, file: &[u8], stateid: &Stateid) -> TestResult<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let at = out.len() as u64;
            let compound = self.sequenced().putfh(file).op(NfsOp4::Read, |ops| {
                put_stateid(ops, stateid);
                ops.put_u64(at);
                ops.put_u32(1 << 20);
            });
            let mut results = self.run_sequenced(compound).await?;
            results.next(NfsOp4::Putfh)?;
            results.next(NfsOp4::Read)?;
            let end = results.reader.bool()?;
            out.extend_from_slice(&results.reader.opaque(BYTES_LIMIT)?);
            if end {
                return Ok(out);
            }
        }
    }

    pub(crate) async fn close(&mut self, file: &[u8], stateid: &Stateid) -> TestResult<()> {
        let compound = self.sequenced().putfh(file).op(NfsOp4::Close, |ops| {
            ops.put_u32(0);
            put_stateid(ops, stateid);
        });
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Close)?;
        results.stateid()?;
        Ok(())
    }

    /// Opens `name` for reading, reads all of it and closes it again.
    pub(crate) async fn read_file(&mut self, parent: &[u8], name: &str) -> TestResult<Vec<u8>> {
        let (stateid, file) = self.open(parent, name, READER).await?;
        let data = self.read(&file, &stateid).await?;
        self.close(&file, &stateid).await?;
        Ok(data)
    }

    /// Renames `old` in the directory `from` to `new` in the directory `to`.
    pub(crate) async fn rename(
        &mut self,
        (from, old): (&[u8], &str),
        (to, new): (&[u8], &str),
    ) -> TestResult<()> {
        let compound = self.sequenced().putfh(from).op(NfsOp4::Savefh, |_| {});
        let compound = compound.putfh(to).op(NfsOp4::Rename, |ops| {
            ops.put_string(old);
            ops.put_string(new);
        });
        let mut results = self.run_sequenced(compound).await?;
        for code in [NfsOp4::Putfh, NfsOp4::Savefh, NfsOp4::Putfh, NfsOp4::Rename] {
            results.next(code)?;
        }
        results.reader.fixed(40)?;
        Ok(())
    }

    pub(crate) async fn remove(&mut self, parent: &[u8], name: &str) -> TestResult<()> {
        let compound = self.sequenced().putfh(parent);
        let compound = compound.op(NfsOp4::Remove, |ops| ops.put_string(name));
        let mut results = self.run_sequenced(compound).await?;
        results.next(NfsOp4::Putfh)?;
        results.next(NfsOp4::Remove)
    }

    /// Whether the server closed this connection, waiting at most one network step.
    pub(crate) async fn closed(&mut self) -> bool {
        let read = time_limit(STEP, read_record(&mut self.stream, RECORD_LIMIT)).await;
        matches!(read, Ok(Ok(None) | Err(_)))
    }
}
