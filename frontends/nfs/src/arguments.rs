//! Decodes the arguments of one COMPOUND operation with bounded XDR; operations are decoded
//! one at a time, so a compound stops at its first undecodable operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;

use crate::attributes::{ClientAttributes, read_bitmap};
use crate::backend::{LockKind, LockRange};
use crate::session::ChannelAttributes;
use crate::types::{
    HANDLE_LIMIT, IO_LIMIT, NfsOp4, NfsStat4, OPAQUE_LIMIT, STATEID_LIMIT, StableHow, Stateid,
};
use crate::xdr::{XdrError, XdrReader};

/// Most callback security entries CREATE_SESSION and BACKCHANNEL_CTL may carry.
const SECURITY_LIMIT: usize = 16;
const MACHINE_LIMIT: usize = 255;
const GROUPS_LIMIT: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceArgs {
    pub session: [u8; 16],
    pub sequence: u32,
    pub slot: u32,
    pub highest: u32,
    pub cache: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeArgs {
    pub verifier: [u8; 8],
    pub owner: Bytes,
    pub flags: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionArgs {
    pub client_id: u64,
    pub sequence: u32,
    pub flags: u32,
    pub fore: ChannelAttributes,
    pub back: ChannelAttributes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaddirArgs {
    pub cookie: u64,
    pub verifier: [u8; 8],
    pub dircount: u32,
    pub maxcount: u32,
    pub request: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenCreate {
    None,
    Unchecked(ClientAttributes),
    Guarded(ClientAttributes),
    Exclusive([u8; 8]),
    /// EXCLUSIVE4_1: a verifier plus attributes inside SUPPATTR_EXCLCREAT.
    ExclusiveAttributes([u8; 8], ClientAttributes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenClaim {
    Null(Bytes),
    Previous,
    DelegateCurrent,
    DelegatePrevious,
    /// CLAIM_FH: the current filehandle is the file.
    Handle,
    /// CLAIM_DELEG_CUR_FH
    DelegateHandle,
    /// CLAIM_DELEG_PREV_FH
    PreviousHandle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenArgs {
    pub access: u32,
    pub deny: u32,
    pub owner: Bytes,
    pub create: OpenCreate,
    pub claim: OpenClaim,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Locker {
    /// The first lock of a lock owner, through an open stateid.
    New {
        open: Stateid,
        owner: Bytes,
    },
    Existing(Stateid),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockArgs {
    pub kind: LockKind,
    pub reclaim: bool,
    pub range: LockRange,
    pub locker: Locker,
}

/// The kind of a CREATE; only directories are supported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateKind {
    Directory,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    Access(u32),
    Close(Stateid),
    Commit {
        offset: u64,
        count: u32,
    },
    Create {
        kind: CreateKind,
        name: Bytes,
        attributes: ClientAttributes,
    },
    Delegreturn,
    Getattr(Vec<u32>),
    Getfh,
    Lock(LockArgs),
    Lockt {
        kind: LockKind,
        range: LockRange,
        owner: Bytes,
    },
    Locku {
        stateid: Stateid,
        range: LockRange,
    },
    Lookup(Bytes),
    Lookupp,
    Nverify(ClientAttributes),
    Open(OpenArgs),
    OpenDowngrade {
        stateid: Stateid,
        access: u32,
        deny: u32,
    },
    Putfh(Bytes),
    Putrootfh,
    Read {
        stateid: Stateid,
        offset: u64,
        count: u32,
    },
    Readdir(ReaddirArgs),
    Readlink,
    Remove(Bytes),
    Rename {
        old: Bytes,
        new: Bytes,
    },
    Restorefh,
    Savefh,
    Secinfo(Bytes),
    Setattr {
        stateid: Stateid,
        attributes: ClientAttributes,
    },
    Verify(ClientAttributes),
    Write {
        stateid: Stateid,
        offset: u64,
        stable: StableHow,
        data: Bytes,
    },
    BackchannelCtl,
    BindConnection {
        session: [u8; 16],
        direction: u32,
    },
    ExchangeId(ExchangeArgs),
    CreateSession(SessionArgs),
    DestroySession([u8; 16]),
    FreeStateid(Stateid),
    SecinfoNoName(u32),
    Sequence(SequenceArgs),
    TestStateid(Vec<Stateid>),
    DestroyClientid(u64),
    ReclaimComplete(bool),
    /// A defined operation this server answers with NOTSUPP; its arguments stay unread.
    Unsupported(NfsOp4),
    Illegal,
}

#[cfg(test)]
impl Operation {
    /// The opcode its result carries; OP_ILLEGAL for an unknown number.
    pub fn code(&self) -> NfsOp4 {
        match self {
            Self::Access(_) => NfsOp4::Access,
            Self::Close(_) => NfsOp4::Close,
            Self::Commit { .. } => NfsOp4::Commit,
            Self::Create { .. } => NfsOp4::Create,
            Self::Delegreturn => NfsOp4::Delegreturn,
            Self::Getattr(_) => NfsOp4::Getattr,
            Self::Getfh => NfsOp4::Getfh,
            Self::Lock(_) => NfsOp4::Lock,
            Self::Lockt { .. } => NfsOp4::Lockt,
            Self::Locku { .. } => NfsOp4::Locku,
            Self::Lookup(_) => NfsOp4::Lookup,
            Self::Lookupp => NfsOp4::Lookupp,
            Self::Nverify(_) => NfsOp4::Nverify,
            Self::Open(_) => NfsOp4::Open,
            Self::OpenDowngrade { .. } => NfsOp4::OpenDowngrade,
            Self::Putfh(_) => NfsOp4::Putfh,
            Self::Putrootfh => NfsOp4::Putrootfh,
            Self::Read { .. } => NfsOp4::Read,
            Self::Readdir(_) => NfsOp4::Readdir,
            Self::Readlink => NfsOp4::Readlink,
            Self::Remove(_) => NfsOp4::Remove,
            Self::Rename { .. } => NfsOp4::Rename,
            Self::Restorefh => NfsOp4::Restorefh,
            Self::Savefh => NfsOp4::Savefh,
            Self::Secinfo(_) => NfsOp4::Secinfo,
            Self::Setattr { .. } => NfsOp4::Setattr,
            Self::Verify(_) => NfsOp4::Verify,
            Self::Write { .. } => NfsOp4::Write,
            Self::BackchannelCtl => NfsOp4::BackchannelCtl,
            Self::BindConnection { .. } => NfsOp4::BindConnection,
            Self::ExchangeId(_) => NfsOp4::ExchangeId,
            Self::CreateSession(_) => NfsOp4::CreateSession,
            Self::DestroySession(_) => NfsOp4::DestroySession,
            Self::FreeStateid(_) => NfsOp4::FreeStateid,
            Self::SecinfoNoName(_) => NfsOp4::SecinfoNoName,
            Self::Sequence(_) => NfsOp4::Sequence,
            Self::TestStateid(_) => NfsOp4::TestStateid,
            Self::DestroyClientid(_) => NfsOp4::DestroyClientid,
            Self::ReclaimComplete(_) => NfsOp4::ReclaimComplete,
            Self::Unsupported(code) => *code,
            Self::Illegal => NfsOp4::Illegal,
        }
    }
}

/// Decodes the arguments of operation `code`. An unknown number is `Illegal`.
pub fn decode(code: u32, reader: &mut XdrReader) -> Result<Operation, NfsStat4> {
    let Some(operation) = NfsOp4::from_code(code) else {
        return Ok(Operation::Illegal);
    };
    let decoded = match operation {
        NfsOp4::Access => Operation::Access(reader.u32()?),
        NfsOp4::Close => {
            reader.u32()?;
            Operation::Close(stateid(reader)?)
        }
        NfsOp4::Commit => Operation::Commit {
            offset: reader.u64()?,
            count: reader.u32()?,
        },
        NfsOp4::Create => {
            let kind = match reader.u32()? {
                2 => CreateKind::Directory,
                5 => {
                    reader.opaque(OPAQUE_LIMIT)?;
                    CreateKind::Other
                }
                3 | 4 => {
                    reader.u32()?;
                    reader.u32()?;
                    CreateKind::Other
                }
                1 | 6 | 7 => CreateKind::Other,
                _ => return Err(NfsStat4::BadType),
            };
            Operation::Create {
                kind,
                name: reader.opaque(OPAQUE_LIMIT)?,
                attributes: ClientAttributes::decode(reader)?,
            }
        }
        NfsOp4::Delegreturn => {
            stateid(reader)?;
            Operation::Delegreturn
        }
        NfsOp4::Getattr => Operation::Getattr(read_bitmap(reader)?),
        NfsOp4::Getfh => Operation::Getfh,
        NfsOp4::Lock => Operation::Lock(lock_args(reader)?),
        NfsOp4::Lockt => {
            let kind = lock_kind(reader.u32()?)?;
            let range = range(reader)?;
            reader.u64()?;
            Operation::Lockt {
                kind,
                range,
                owner: reader.opaque(OPAQUE_LIMIT)?,
            }
        }
        NfsOp4::Locku => {
            lock_kind(reader.u32()?)?;
            reader.u32()?;
            Operation::Locku {
                stateid: stateid(reader)?,
                range: range(reader)?,
            }
        }
        NfsOp4::Lookup => Operation::Lookup(reader.opaque(OPAQUE_LIMIT)?),
        NfsOp4::Lookupp => Operation::Lookupp,
        NfsOp4::Nverify => Operation::Nverify(ClientAttributes::decode(reader)?),
        NfsOp4::Open => Operation::Open(open_args(reader)?),
        NfsOp4::OpenDowngrade => {
            let stateid = stateid(reader)?;
            reader.u32()?;
            Operation::OpenDowngrade {
                stateid,
                access: reader.u32()?,
                deny: reader.u32()?,
            }
        }
        NfsOp4::Putfh => Operation::Putfh(reader.opaque(HANDLE_LIMIT)?),
        NfsOp4::Putrootfh | NfsOp4::Putpubfh => Operation::Putrootfh,
        NfsOp4::Read => Operation::Read {
            stateid: stateid(reader)?,
            offset: reader.u64()?,
            count: reader.u32()?,
        },
        NfsOp4::Readdir => Operation::Readdir(ReaddirArgs {
            cookie: reader.u64()?,
            verifier: reader.array::<8>()?,
            dircount: reader.u32()?,
            maxcount: reader.u32()?,
            request: read_bitmap(reader)?,
        }),
        NfsOp4::Readlink => Operation::Readlink,
        NfsOp4::Remove => Operation::Remove(reader.opaque(OPAQUE_LIMIT)?),
        NfsOp4::Rename => Operation::Rename {
            old: reader.opaque(OPAQUE_LIMIT)?,
            new: reader.opaque(OPAQUE_LIMIT)?,
        },
        NfsOp4::Restorefh => Operation::Restorefh,
        NfsOp4::Savefh => Operation::Savefh,
        NfsOp4::Secinfo => Operation::Secinfo(reader.opaque(OPAQUE_LIMIT)?),
        NfsOp4::Setattr => Operation::Setattr {
            stateid: stateid(reader)?,
            attributes: ClientAttributes::decode(reader)?,
        },
        NfsOp4::Verify => Operation::Verify(ClientAttributes::decode(reader)?),
        NfsOp4::Write => Operation::Write {
            stateid: stateid(reader)?,
            offset: reader.u64()?,
            stable: StableHow::from_code(reader.u32()?).ok_or(NfsStat4::BadXdr)?,
            data: reader.opaque(IO_LIMIT as usize)?,
        },
        NfsOp4::BackchannelCtl => {
            reader.u32()?;
            security_parameters(reader)?;
            Operation::BackchannelCtl
        }
        NfsOp4::BindConnection => {
            let session = reader.array::<16>()?;
            let direction = reader.u32()?;
            reader.bool()?;
            Operation::BindConnection { session, direction }
        }
        NfsOp4::ExchangeId => Operation::ExchangeId(exchange_args(reader)?),
        NfsOp4::CreateSession => Operation::CreateSession(session_args(reader)?),
        NfsOp4::DestroySession => Operation::DestroySession(reader.array::<16>()?),
        NfsOp4::FreeStateid => Operation::FreeStateid(stateid(reader)?),
        NfsOp4::SecinfoNoName => Operation::SecinfoNoName(reader.u32()?),
        NfsOp4::Sequence => Operation::Sequence(SequenceArgs {
            session: reader.array::<16>()?,
            sequence: reader.u32()?,
            slot: reader.u32()?,
            highest: reader.u32()?,
            cache: reader.bool()?,
        }),
        NfsOp4::TestStateid => {
            let count = reader.count(STATEID_LIMIT)?;
            let mut stateids = Vec::with_capacity(count);
            for _ in 0..count {
                stateids.push(stateid(reader)?);
            }
            Operation::TestStateid(stateids)
        }
        NfsOp4::DestroyClientid => Operation::DestroyClientid(reader.u64()?),
        NfsOp4::ReclaimComplete => Operation::ReclaimComplete(reader.bool()?),
        other => Operation::Unsupported(other),
    };
    Ok(decoded)
}

pub fn stateid(reader: &mut XdrReader) -> Result<Stateid, XdrError> {
    Ok(Stateid {
        seqid: reader.u32()?,
        other: reader.array::<12>()?,
    })
}

fn lock_kind(code: u32) -> Result<LockKind, NfsStat4> {
    match code {
        1 | 3 => Ok(LockKind::Read),
        2 | 4 => Ok(LockKind::Write),
        _ => Err(NfsStat4::Invalid),
    }
}

fn range(reader: &mut XdrReader) -> Result<LockRange, XdrError> {
    Ok(LockRange {
        offset: reader.u64()?,
        length: reader.u64()?,
    })
}

fn lock_args(reader: &mut XdrReader) -> Result<LockArgs, NfsStat4> {
    let kind = lock_kind(reader.u32()?)?;
    let reclaim = reader.bool()?;
    let range = range(reader)?;
    let locker = if reader.bool()? {
        reader.u32()?;
        let open = stateid(reader)?;
        reader.u32()?;
        reader.u64()?;
        Locker::New {
            open,
            owner: reader.opaque(OPAQUE_LIMIT)?,
        }
    } else {
        let existing = stateid(reader)?;
        reader.u32()?;
        Locker::Existing(existing)
    };
    Ok(LockArgs {
        kind,
        reclaim,
        range,
        locker,
    })
}

/// OPEN arguments. The open-owner seqid and client id are read and ignored, as NFSv4.1 does.
fn open_args(reader: &mut XdrReader) -> Result<OpenArgs, NfsStat4> {
    reader.u32()?;
    let access = reader.u32()?;
    let deny = reader.u32()?;
    reader.u64()?;
    let owner = reader.opaque(OPAQUE_LIMIT)?;
    let create = match reader.u32()? {
        0 => OpenCreate::None,
        1 => match reader.u32()? {
            0 => OpenCreate::Unchecked(ClientAttributes::decode(reader)?),
            1 => OpenCreate::Guarded(ClientAttributes::decode(reader)?),
            2 => OpenCreate::Exclusive(reader.array::<8>()?),
            3 => {
                let verifier = reader.array::<8>()?;
                OpenCreate::ExclusiveAttributes(verifier, ClientAttributes::decode(reader)?)
            }
            _ => return Err(NfsStat4::BadXdr),
        },
        _ => return Err(NfsStat4::BadXdr),
    };
    let claim = match reader.u32()? {
        0 => OpenClaim::Null(reader.opaque(OPAQUE_LIMIT)?),
        1 => {
            reader.u32()?;
            OpenClaim::Previous
        }
        2 => {
            stateid(reader)?;
            reader.opaque(OPAQUE_LIMIT)?;
            OpenClaim::DelegateCurrent
        }
        3 => {
            reader.opaque(OPAQUE_LIMIT)?;
            OpenClaim::DelegatePrevious
        }
        4 => OpenClaim::Handle,
        5 => {
            stateid(reader)?;
            OpenClaim::DelegateHandle
        }
        6 => OpenClaim::PreviousHandle,
        _ => return Err(NfsStat4::BadXdr),
    };
    Ok(OpenArgs {
        access,
        deny,
        owner,
        create,
        claim,
    })
}

fn exchange_args(reader: &mut XdrReader) -> Result<ExchangeArgs, NfsStat4> {
    let verifier = reader.array::<8>()?;
    let owner = reader.opaque(OPAQUE_LIMIT)?;
    let flags = reader.u32()?;
    match reader.u32()? {
        0 => {}
        1 => {
            read_bitmap(reader)?;
            read_bitmap(reader)?;
        }
        2 => return Err(NfsStat4::EncryptionUnsupported),
        _ => return Err(NfsStat4::BadXdr),
    }
    if reader.count(1)? == 1 {
        reader.string(OPAQUE_LIMIT)?;
        reader.string(OPAQUE_LIMIT)?;
        reader.i64()?;
        reader.u32()?;
    }
    Ok(ExchangeArgs {
        verifier,
        owner,
        flags,
    })
}

fn channel(reader: &mut XdrReader) -> Result<ChannelAttributes, XdrError> {
    let attributes = ChannelAttributes {
        header_pad: reader.u32()?,
        max_request: reader.u32()?,
        max_response: reader.u32()?,
        max_cached: reader.u32()?,
        max_operations: reader.u32()?,
        max_requests: reader.u32()?,
    };
    if reader.count(1)? == 1 {
        reader.u32()?;
    }
    Ok(attributes)
}

fn session_args(reader: &mut XdrReader) -> Result<SessionArgs, NfsStat4> {
    let client_id = reader.u64()?;
    let sequence = reader.u32()?;
    let flags = reader.u32()?;
    let fore = channel(reader)?;
    let back = channel(reader)?;
    reader.u32()?;
    security_parameters(reader)?;
    Ok(SessionArgs {
        client_id,
        sequence,
        flags,
        fore,
        back,
    })
}

/// Reads and drops `callback_sec_parms4<>`; the server opens no back channel.
fn security_parameters(reader: &mut XdrReader) -> Result<(), NfsStat4> {
    let count = reader.count(SECURITY_LIMIT)?;
    for _ in 0..count {
        match reader.u32()? {
            0 => {}
            1 => {
                reader.u32()?;
                reader.string(MACHINE_LIMIT)?;
                reader.u32()?;
                reader.u32()?;
                let groups = reader.count(GROUPS_LIMIT)?;
                for _ in 0..groups {
                    reader.u32()?;
                }
            }
            6 => {
                reader.u32()?;
                reader.opaque(OPAQUE_LIMIT)?;
                reader.opaque(OPAQUE_LIMIT)?;
            }
            _ => return Err(NfsStat4::BadXdr),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xdr::XdrWriter;

    #[test]
    fn unknown_opcode_illegal() {
        let mut reader = XdrReader::new(Bytes::new());
        assert_eq!(decode(2, &mut reader), Ok(Operation::Illegal));
        assert_eq!(decode(9999, &mut reader), Ok(Operation::Illegal));
        assert_eq!(Operation::Illegal.code(), NfsOp4::Illegal);
    }

    // A truncated argument is BADXDR, and an oversized handle is refused before it is read.
    #[test]
    fn bounded_arguments_refused() {
        let mut writer = XdrWriter::new();
        writer.put_u32(200);
        let mut reader = XdrReader::new(writer.into_bytes());
        assert_eq!(
            decode(NfsOp4::Putfh.code(), &mut reader),
            Err(NfsStat4::BadXdr)
        );
        let mut reader = XdrReader::new(Bytes::from_static(&[0, 0]));
        assert_eq!(
            decode(NfsOp4::Access.code(), &mut reader),
            Err(NfsStat4::BadXdr)
        );
    }

    #[test]
    fn decodes_sequence() {
        let mut writer = XdrWriter::new();
        writer.put_fixed(&[3; 16]);
        writer.put_u32(5);
        writer.put_u32(1);
        writer.put_u32(1);
        writer.put_bool(true);
        let mut reader = XdrReader::new(writer.into_bytes());
        let decoded = decode(NfsOp4::Sequence.code(), &mut reader).expect("sequence");
        assert_eq!(
            decoded,
            Operation::Sequence(SequenceArgs {
                session: [3; 16],
                sequence: 5,
                slot: 1,
                highest: 1,
                cache: true,
            })
        );
    }
}
