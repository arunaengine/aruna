//! ONC RPC (RFC 5531) call parsing and reply framing with a closed credential set: AUTH_SYS,
//! RPCSEC_GSS through an acceptor, and the RFC 9289 AUTH_TLS probe.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;

use crate::xdr::{XdrReader, XdrWriter};

pub const RPC_VERSION: u32 = 2;
pub const NFS_PROGRAM: u32 = 100_003;
pub const NFS_VERSION: u32 = 4;
pub const NULL_PROCEDURE: u32 = 0;
pub const COMPOUND_PROCEDURE: u32 = 1;
const CALL: u32 = 0;
const REPLY: u32 = 1;
const ACCEPTED: u32 = 0;
const DENIED: u32 = 1;
const RPC_MISMATCH: u32 = 0;
const AUTH_ERROR: u32 = 1;
/// Largest credential or verifier body (`MAX_AUTH_BYTES`).
const AUTH_LIMIT: usize = 400;
const MACHINE_LIMIT: usize = 255;
const GROUPS_LIMIT: usize = 16;
/// The verifier body that tells an RFC 9289 client to start TLS.
pub const STARTTLS: &[u8] = b"STARTTLS";

/// Authentication flavors (`AUTH_*`).
pub mod flavor {
    pub const NONE: u32 = 0;
    pub const SYS: u32 = 1;
    pub const GSS: u32 = 6;
    pub const TLS: u32 = 7;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum AcceptStat {
    Success = 0,
    ProgramUnavailable = 1,
    ProgramMismatch = 2,
    ProcedureUnavailable = 3,
    GarbageArgs = 4,
    SystemError = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum AuthStat {
    BadCredential = 1,
    RejectedCredential = 2,
    BadVerifier = 3,
    RejectedVerifier = 4,
    TooWeak = 5,
    InvalidResponse = 6,
    Failed = 7,
    /// RPCSEC_GSS_CREDPROBLEM
    CredentialProblem = 13,
    /// RPCSEC_GSS_CTXPROBLEM
    ContextProblem = 14,
}

/// An AUTH_SYS credential. Its numbers only describe the caller; Aruna rights come from the
/// enrolled host and the mount binding alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthSys {
    pub stamp: u32,
    pub machine: String,
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

/// An RPCSEC_GSS version 1 credential (RFC 2203).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GssHeader {
    pub version: u32,
    pub procedure: u32,
    pub sequence: u32,
    pub service: u32,
    pub context: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcCred {
    None,
    Sys(AuthSys),
    Gss(GssHeader),
    /// The RFC 9289 AUTH_TLS probe credential; valid only on the cleartext NULL probe.
    Tls,
}

/// Who sent a call, as the RPC layer names it; a cached reply is released only to the same one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RpcPrincipal {
    Anonymous,
    Sys { uid: u32, gid: u32 },
    Gss(Bytes),
}

#[derive(Clone, Debug)]
pub struct RpcCall {
    pub xid: u32,
    pub program: u32,
    pub version: u32,
    pub procedure: u32,
    pub cred: RpcCred,
    pub verifier_flavor: u32,
    pub body: Bytes,
}

impl RpcCall {
    /// Whether this is the RFC 9289 STARTTLS probe: a NULL call with an AUTH_TLS credential.
    pub fn is_probe(&self) -> bool {
        self.procedure == NULL_PROCEDURE
            && self.cred == RpcCred::Tls
            && self.verifier_flavor == flavor::NONE
    }
}

/// Why a record is not an acceptable call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallFault {
    /// Not even a call header; the connection closes.
    Unreadable,
    Mismatch {
        xid: u32,
    },
    Auth {
        xid: u32,
        stat: AuthStat,
    },
}

/// Accepts or refuses RPCSEC_GSS credentials. Kerberos lives behind this seam, so the default
/// AUTH_SYS over mutual TLS profile needs no C library.
pub trait GssAcceptor: Send + Sync {
    /// The principal and the reply verifier body of an accepted credential.
    fn accept(&self, header: &GssHeader) -> Result<(RpcPrincipal, Bytes), AuthStat>;
}

/// Refuses every RPCSEC_GSS credential.
#[derive(Clone, Copy, Debug, Default)]
pub struct DisabledGssAcceptor;

impl GssAcceptor for DisabledGssAcceptor {
    fn accept(&self, _header: &GssHeader) -> Result<(RpcPrincipal, Bytes), AuthStat> {
        Err(AuthStat::BadCredential)
    }
}

pub fn parse_call(record: Bytes) -> Result<RpcCall, CallFault> {
    let mut reader = XdrReader::new(record);
    let xid = reader.u32().map_err(|_| CallFault::Unreadable)?;
    if reader.u32().map_err(|_| CallFault::Unreadable)? != CALL {
        return Err(CallFault::Unreadable);
    }
    let garbage = |_| CallFault::Auth {
        xid,
        stat: AuthStat::BadCredential,
    };
    if reader.u32().map_err(garbage)? != RPC_VERSION {
        return Err(CallFault::Mismatch { xid });
    }
    let program = reader.u32().map_err(garbage)?;
    let version = reader.u32().map_err(garbage)?;
    let procedure = reader.u32().map_err(garbage)?;
    let cred_flavor = reader.u32().map_err(garbage)?;
    let cred_body = reader.opaque(AUTH_LIMIT).map_err(garbage)?;
    let verifier_flavor = reader.u32().map_err(garbage)?;
    reader.opaque(AUTH_LIMIT).map_err(garbage)?;
    let cred = match cred_flavor {
        flavor::NONE => RpcCred::None,
        flavor::SYS => RpcCred::Sys(parse_sys(cred_body).ok_or(CallFault::Auth {
            xid,
            stat: AuthStat::BadCredential,
        })?),
        flavor::GSS => RpcCred::Gss(parse_gss(cred_body).ok_or(CallFault::Auth {
            xid,
            stat: AuthStat::BadCredential,
        })?),
        flavor::TLS => RpcCred::Tls,
        _ => {
            return Err(CallFault::Auth {
                xid,
                stat: AuthStat::BadCredential,
            });
        }
    };
    Ok(RpcCall {
        xid,
        program,
        version,
        procedure,
        cred,
        verifier_flavor,
        body: reader.into_rest(),
    })
}

fn parse_sys(body: Bytes) -> Option<AuthSys> {
    let mut reader = XdrReader::new(body);
    let stamp = reader.u32().ok()?;
    let machine = reader.string(MACHINE_LIMIT).ok()?;
    let uid = reader.u32().ok()?;
    let gid = reader.u32().ok()?;
    let count = reader.count(GROUPS_LIMIT).ok()?;
    let mut groups = Vec::with_capacity(count);
    for _ in 0..count {
        groups.push(reader.u32().ok()?);
    }
    (reader.remaining() == 0).then_some(AuthSys {
        stamp,
        machine,
        uid,
        gid,
        groups,
    })
}

fn parse_gss(body: Bytes) -> Option<GssHeader> {
    let mut reader = XdrReader::new(body);
    let header = GssHeader {
        version: reader.u32().ok()?,
        procedure: reader.u32().ok()?,
        sequence: reader.u32().ok()?,
        service: reader.u32().ok()?,
        context: reader.opaque(AUTH_LIMIT).ok()?,
    };
    (reader.remaining() == 0).then_some(header)
}

/// A MSG_ACCEPTED reply with the given verifier, status and body.
pub fn accepted_reply(xid: u32, verifier: (u32, &[u8]), stat: AcceptStat, body: &[u8]) -> Bytes {
    let mut writer = XdrWriter::new();
    writer.put_u32(xid);
    writer.put_u32(REPLY);
    writer.put_u32(ACCEPTED);
    writer.put_u32(verifier.0);
    writer.put_opaque(verifier.1);
    writer.put_u32(stat as u32);
    if stat == AcceptStat::ProgramMismatch {
        writer.put_u32(NFS_VERSION);
        writer.put_u32(NFS_VERSION);
    }
    writer.put_raw(body);
    writer.into_bytes()
}

/// A successful reply with an AUTH_NONE verifier.
pub fn success_reply(xid: u32, body: &[u8]) -> Bytes {
    accepted_reply(xid, (flavor::NONE, &[]), AcceptStat::Success, body)
}

/// The RFC 9289 answer to the probe: success with the `STARTTLS` AUTH_NONE verifier.
pub fn starttls_reply(xid: u32) -> Bytes {
    accepted_reply(xid, (flavor::NONE, STARTTLS), AcceptStat::Success, &[])
}

pub fn status_reply(xid: u32, stat: AcceptStat) -> Bytes {
    accepted_reply(xid, (flavor::NONE, &[]), stat, &[])
}

pub fn denied_reply(xid: u32, stat: AuthStat) -> Bytes {
    let mut writer = XdrWriter::new();
    writer.put_u32(xid);
    writer.put_u32(REPLY);
    writer.put_u32(DENIED);
    writer.put_u32(AUTH_ERROR);
    writer.put_u32(stat as u32);
    writer.into_bytes()
}

pub fn mismatch_reply(xid: u32) -> Bytes {
    let mut writer = XdrWriter::new();
    writer.put_u32(xid);
    writer.put_u32(REPLY);
    writer.put_u32(DENIED);
    writer.put_u32(RPC_MISMATCH);
    writer.put_u32(RPC_VERSION);
    writer.put_u32(RPC_VERSION);
    writer.into_bytes()
}

/// Builds a call record for tests and for the probe check.
#[cfg(test)]
pub(crate) fn encode_call(xid: u32, procedure: u32, cred: &RpcCred, body: &[u8]) -> Bytes {
    let mut writer = XdrWriter::new();
    writer.put_u32(xid);
    writer.put_u32(CALL);
    writer.put_u32(RPC_VERSION);
    writer.put_u32(NFS_PROGRAM);
    writer.put_u32(NFS_VERSION);
    writer.put_u32(procedure);
    match cred {
        RpcCred::None => {
            writer.put_u32(flavor::NONE);
            writer.put_opaque(&[]);
        }
        RpcCred::Tls => {
            writer.put_u32(flavor::TLS);
            writer.put_opaque(&[]);
        }
        RpcCred::Sys(sys) => {
            let mut inner = XdrWriter::new();
            inner.put_u32(sys.stamp);
            inner.put_string(&sys.machine);
            inner.put_u32(sys.uid);
            inner.put_u32(sys.gid);
            inner.put_u32(sys.groups.len() as u32);
            for group in &sys.groups {
                inner.put_u32(*group);
            }
            writer.put_u32(flavor::SYS);
            writer.put_opaque(inner.as_slice());
        }
        RpcCred::Gss(header) => {
            let mut inner = XdrWriter::new();
            inner.put_u32(header.version);
            inner.put_u32(header.procedure);
            inner.put_u32(header.sequence);
            inner.put_u32(header.service);
            inner.put_opaque(&header.context);
            writer.put_u32(flavor::GSS);
            writer.put_opaque(inner.as_slice());
        }
    }
    writer.put_u32(flavor::NONE);
    writer.put_opaque(&[]);
    writer.put_raw(body);
    writer.into_bytes()
}

/// A parsed reply header for tests: the xid, whether it was accepted, the verifier body and
/// the status word, followed by the remaining body.
#[cfg(test)]
pub(crate) fn parse_reply(record: Bytes) -> (u32, bool, Bytes, u32, XdrReader) {
    let mut reader = XdrReader::new(record);
    let xid = reader.u32().expect("xid");
    assert_eq!(reader.u32().expect("type"), REPLY);
    let accepted = reader.u32().expect("reply state") == ACCEPTED;
    if !accepted {
        let reason = reader.u32().expect("reject reason");
        let stat = reader.u32().expect("detail");
        return (xid, false, Bytes::new(), reason << 16 | stat, reader);
    }
    reader.u32().expect("verifier flavor");
    let verifier = reader.opaque(AUTH_LIMIT).expect("verifier");
    let stat = reader.u32().expect("accept status");
    (xid, true, verifier, stat, reader)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sys() -> RpcCred {
        RpcCred::Sys(AuthSys {
            stamp: 1,
            machine: "client".to_string(),
            uid: 1000,
            gid: 1000,
            groups: vec![10, 20],
        })
    }

    #[test]
    fn parses_sys_call() {
        let record = encode_call(9, COMPOUND_PROCEDURE, &sys(), b"body");
        let call = parse_call(record).expect("call");
        assert_eq!(call.xid, 9);
        assert_eq!(call.cred, sys());
        assert_eq!(&call.body[..], b"body");
        assert!(!call.is_probe());
    }

    #[test]
    fn recognizes_tls_probe() {
        let call = parse_call(encode_call(4, NULL_PROCEDURE, &RpcCred::Tls, &[])).expect("call");
        assert!(call.is_probe());
        let (xid, accepted, verifier, stat, _) = parse_reply(starttls_reply(4));
        assert_eq!((xid, accepted, stat), (4, true, 0));
        assert_eq!(&verifier[..], STARTTLS);
    }

    // Too many AUTH_SYS groups make the credential itself bad, answered with its xid.
    #[test]
    fn rejects_oversized_groups() {
        let cred = RpcCred::Sys(AuthSys {
            stamp: 1,
            machine: "client".to_string(),
            uid: 0,
            gid: 0,
            groups: vec![1; 17],
        });
        let fault = parse_call(encode_call(3, COMPOUND_PROCEDURE, &cred, &[])).unwrap_err();
        assert_eq!(
            fault,
            CallFault::Auth {
                xid: 3,
                stat: AuthStat::BadCredential
            }
        );
    }

    #[test]
    fn disabled_gss_refuses() {
        let header = GssHeader {
            version: 1,
            procedure: 0,
            sequence: 1,
            service: 1,
            context: Bytes::new(),
        };
        assert_eq!(
            DisabledGssAcceptor.accept(&header),
            Err(AuthStat::BadCredential)
        );
        let call = parse_call(encode_call(5, 1, &RpcCred::Gss(header.clone()), &[])).expect("gss");
        assert_eq!(call.cred, RpcCred::Gss(header));
    }
}
