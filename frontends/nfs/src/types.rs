//! NFSv4.1 protocol numbers of RFC 8881: status codes, operations, file types, attribute bits,
//! special stateids and the sequence status flags.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// NFSv4 opaque limit (`NFS4_OPAQUE_LIMIT`) for owners, tags and server scopes.
pub const OPAQUE_LIMIT: usize = 1024;
/// Largest filehandle a client may send (`NFS4_FHSIZE`).
pub const HANDLE_LIMIT: usize = 128;
/// Longest component name the server accepts, in bytes.
pub const NAME_LIMIT: usize = 255;
/// Largest READ or WRITE payload in bytes.
pub const IO_LIMIT: u32 = 1024 * 1024;
/// Most stateids one TEST_STATEID may carry.
pub const STATEID_LIMIT: usize = 1024;

/// `nfsstat4`. Names follow the RFC constants; the doc names the constant where it differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum NfsStat4 {
    Ok = 0,
    /// NFS4ERR_PERM
    NotOwner = 1,
    /// NFS4ERR_NOENT
    NoEntry = 2,
    Io = 5,
    /// NFS4ERR_NXIO
    NoDevice = 6,
    Access = 13,
    Exist = 17,
    Xdev = 18,
    /// NFS4ERR_NOTDIR
    NotDirectory = 20,
    /// NFS4ERR_ISDIR
    IsDirectory = 21,
    /// NFS4ERR_INVAL
    Invalid = 22,
    /// NFS4ERR_FBIG
    FileTooBig = 27,
    /// NFS4ERR_NOSPC
    NoSpace = 28,
    /// NFS4ERR_ROFS
    ReadOnly = 30,
    /// NFS4ERR_MLINK
    TooManyLinks = 31,
    NameTooLong = 63,
    NotEmpty = 66,
    /// NFS4ERR_DQUOT
    QuotaExceeded = 69,
    Stale = 70,
    BadHandle = 10001,
    BadCookie = 10003,
    /// NFS4ERR_NOTSUPP
    NotSupported = 10004,
    TooSmall = 10005,
    ServerFault = 10006,
    BadType = 10007,
    Delay = 10008,
    Same = 10009,
    Denied = 10010,
    Expired = 10011,
    Locked = 10012,
    Grace = 10013,
    /// NFS4ERR_FHEXPIRED
    FilehandleExpired = 10014,
    ShareDenied = 10015,
    /// NFS4ERR_WRONGSEC
    WrongSecurity = 10016,
    /// NFS4ERR_CLID_INUSE
    ClientInUse = 10017,
    Resource = 10018,
    Moved = 10019,
    /// NFS4ERR_NOFILEHANDLE
    NoFilehandle = 10020,
    /// NFS4ERR_MINOR_VERS_MISMATCH
    MinorMismatch = 10021,
    /// NFS4ERR_STALE_CLIENTID
    StaleClient = 10022,
    StaleStateid = 10023,
    OldStateid = 10024,
    BadStateid = 10025,
    BadSeqid = 10026,
    NotSame = 10027,
    LockRange = 10028,
    Symlink = 10029,
    /// NFS4ERR_RESTOREFH
    RestoreFilehandle = 10030,
    LeaseMoved = 10031,
    /// NFS4ERR_ATTRNOTSUPP
    AttributeUnsupported = 10032,
    NoGrace = 10033,
    ReclaimBad = 10034,
    ReclaimConflict = 10035,
    BadXdr = 10036,
    LocksHeld = 10037,
    OpenMode = 10038,
    BadOwner = 10039,
    BadChar = 10040,
    BadName = 10041,
    BadRange = 10042,
    /// NFS4ERR_LOCK_NOTSUPP
    LockUnsupported = 10043,
    /// NFS4ERR_OP_ILLEGAL
    OperationIllegal = 10044,
    Deadlock = 10045,
    FileOpen = 10046,
    AdminRevoked = 10047,
    /// NFS4ERR_CB_PATH_DOWN
    CallbackDown = 10048,
    BadIomode = 10049,
    BadLayout = 10050,
    BadSessionDigest = 10051,
    /// NFS4ERR_BADSESSION
    BadSession = 10052,
    /// NFS4ERR_BADSLOT
    BadSlot = 10053,
    CompleteAlready = 10054,
    /// NFS4ERR_CONN_NOT_BOUND_TO_SESSION
    ConnectionNotBound = 10055,
    /// NFS4ERR_DELEG_ALREADY_WANTED
    DelegationAlreadyWanted = 10056,
    /// NFS4ERR_BACK_CHAN_BUSY
    BackchannelBusy = 10057,
    LayoutTryLater = 10058,
    LayoutUnavailable = 10059,
    /// NFS4ERR_NOMATCHING_LAYOUT
    NoMatchingLayout = 10060,
    RecallConflict = 10061,
    /// NFS4ERR_UNKNOWN_LAYOUTTYPE
    UnknownLayout = 10062,
    /// NFS4ERR_SEQ_MISORDERED
    SequenceMisordered = 10063,
    /// NFS4ERR_SEQUENCE_POS
    SequencePosition = 10064,
    /// NFS4ERR_REQ_TOO_BIG
    RequestTooBig = 10065,
    /// NFS4ERR_REP_TOO_BIG
    ReplyTooBig = 10066,
    /// NFS4ERR_REP_TOO_BIG_TO_CACHE
    ReplyUncacheable = 10067,
    /// NFS4ERR_RETRY_UNCACHED_REP
    RetryUncached = 10068,
    UnsafeCompound = 10069,
    /// NFS4ERR_TOO_MANY_OPS
    TooManyOperations = 10070,
    /// NFS4ERR_OP_NOT_IN_SESSION
    NotInSession = 10071,
    /// NFS4ERR_HASH_ALG_UNSUPP
    HashUnsupported = 10072,
    ClientBusy = 10074,
    /// NFS4ERR_PNFS_IO_HOLE
    LayoutHole = 10075,
    /// NFS4ERR_SEQ_FALSE_RETRY
    FalseRetry = 10076,
    BadHighSlot = 10077,
    DeadSession = 10078,
    /// NFS4ERR_ENCR_ALG_UNSUPP
    EncryptionUnsupported = 10079,
    /// NFS4ERR_PNFS_NO_LAYOUT
    NoLayout = 10080,
    /// NFS4ERR_NOT_ONLY_OP
    NotOnlyOperation = 10081,
    /// NFS4ERR_WRONG_CRED
    WrongCredential = 10082,
    WrongType = 10083,
    /// NFS4ERR_DIRDELEG_UNAVAIL
    DirectoryDelegationUnavailable = 10084,
    /// NFS4ERR_REJECT_DELEG
    RejectDelegation = 10085,
    ReturnConflict = 10086,
    /// NFS4ERR_DELEG_REVOKED
    DelegationRevoked = 10087,
}

impl NfsStat4 {
    pub fn code(self) -> u32 {
        self as u32
    }
}

/// `nfs_opnum4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum NfsOp4 {
    Access = 3,
    Close = 4,
    Commit = 5,
    Create = 6,
    Delegpurge = 7,
    Delegreturn = 8,
    Getattr = 9,
    Getfh = 10,
    Link = 11,
    Lock = 12,
    Lockt = 13,
    Locku = 14,
    Lookup = 15,
    Lookupp = 16,
    Nverify = 17,
    Open = 18,
    Openattr = 19,
    OpenConfirm = 20,
    OpenDowngrade = 21,
    Putfh = 22,
    Putpubfh = 23,
    Putrootfh = 24,
    Read = 25,
    Readdir = 26,
    Readlink = 27,
    Remove = 28,
    Rename = 29,
    Renew = 30,
    Restorefh = 31,
    Savefh = 32,
    Secinfo = 33,
    Setattr = 34,
    Setclientid = 35,
    SetclientidConfirm = 36,
    Verify = 37,
    Write = 38,
    ReleaseLockowner = 39,
    BackchannelCtl = 40,
    /// OP_BIND_CONN_TO_SESSION
    BindConnection = 41,
    ExchangeId = 42,
    CreateSession = 43,
    DestroySession = 44,
    FreeStateid = 45,
    GetDirDelegation = 46,
    Getdeviceinfo = 47,
    Getdevicelist = 48,
    Layoutcommit = 49,
    Layoutget = 50,
    Layoutreturn = 51,
    SecinfoNoName = 52,
    Sequence = 53,
    SetSsv = 54,
    TestStateid = 55,
    WantDelegation = 56,
    DestroyClientid = 57,
    ReclaimComplete = 58,
    Illegal = 10044,
}

impl NfsOp4 {
    /// The operation of a wire number; `None` is OP_ILLEGAL.
    pub fn from_code(code: u32) -> Option<Self> {
        const ALL: [NfsOp4; 56] = [
            NfsOp4::Access,
            NfsOp4::Close,
            NfsOp4::Commit,
            NfsOp4::Create,
            NfsOp4::Delegpurge,
            NfsOp4::Delegreturn,
            NfsOp4::Getattr,
            NfsOp4::Getfh,
            NfsOp4::Link,
            NfsOp4::Lock,
            NfsOp4::Lockt,
            NfsOp4::Locku,
            NfsOp4::Lookup,
            NfsOp4::Lookupp,
            NfsOp4::Nverify,
            NfsOp4::Open,
            NfsOp4::Openattr,
            NfsOp4::OpenConfirm,
            NfsOp4::OpenDowngrade,
            NfsOp4::Putfh,
            NfsOp4::Putpubfh,
            NfsOp4::Putrootfh,
            NfsOp4::Read,
            NfsOp4::Readdir,
            NfsOp4::Readlink,
            NfsOp4::Remove,
            NfsOp4::Rename,
            NfsOp4::Renew,
            NfsOp4::Restorefh,
            NfsOp4::Savefh,
            NfsOp4::Secinfo,
            NfsOp4::Setattr,
            NfsOp4::Setclientid,
            NfsOp4::SetclientidConfirm,
            NfsOp4::Verify,
            NfsOp4::Write,
            NfsOp4::ReleaseLockowner,
            NfsOp4::BackchannelCtl,
            NfsOp4::BindConnection,
            NfsOp4::ExchangeId,
            NfsOp4::CreateSession,
            NfsOp4::DestroySession,
            NfsOp4::FreeStateid,
            NfsOp4::GetDirDelegation,
            NfsOp4::Getdeviceinfo,
            NfsOp4::Getdevicelist,
            NfsOp4::Layoutcommit,
            NfsOp4::Layoutget,
            NfsOp4::Layoutreturn,
            NfsOp4::SecinfoNoName,
            NfsOp4::Sequence,
            NfsOp4::SetSsv,
            NfsOp4::TestStateid,
            NfsOp4::WantDelegation,
            NfsOp4::DestroyClientid,
            NfsOp4::ReclaimComplete,
        ];
        let index = code.checked_sub(3)? as usize;
        ALL.get(index).copied()
    }

    pub fn code(self) -> u32 {
        self as u32
    }
}

/// `nfs_ftype4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum NfsType4 {
    Regular = 1,
    Directory = 2,
    Block = 3,
    Character = 4,
    Link = 5,
    Socket = 6,
    Fifo = 7,
    AttributeDirectory = 8,
    NamedAttribute = 9,
}

/// `stable_how4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum StableHow {
    Unstable = 0,
    DataSync = 1,
    FileSync = 2,
}

impl StableHow {
    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            0 => Some(Self::Unstable),
            1 => Some(Self::DataSync),
            2 => Some(Self::FileSync),
            _ => None,
        }
    }
}

/// `nfstime4`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct NfsTime {
    pub seconds: i64,
    pub nanos: u32,
}

/// Attribute bit numbers (`FATTR4_*`).
pub mod attribute {
    pub const SUPPORTED_ATTRS: u32 = 0;
    pub const TYPE: u32 = 1;
    pub const FH_EXPIRE_TYPE: u32 = 2;
    pub const CHANGE: u32 = 3;
    pub const SIZE: u32 = 4;
    pub const LINK_SUPPORT: u32 = 5;
    pub const SYMLINK_SUPPORT: u32 = 6;
    pub const NAMED_ATTR: u32 = 7;
    pub const FSID: u32 = 8;
    pub const UNIQUE_HANDLES: u32 = 9;
    pub const LEASE_TIME: u32 = 10;
    pub const RDATTR_ERROR: u32 = 11;
    pub const FILEHANDLE: u32 = 19;
    pub const FILEID: u32 = 20;
    pub const MAXFILESIZE: u32 = 27;
    pub const MAXNAME: u32 = 29;
    pub const MAXREAD: u32 = 30;
    pub const MAXWRITE: u32 = 31;
    pub const MODE: u32 = 33;
    pub const NUMLINKS: u32 = 35;
    pub const OWNER: u32 = 36;
    pub const OWNER_GROUP: u32 = 37;
    pub const RAWDEV: u32 = 41;
    pub const SPACE_USED: u32 = 45;
    pub const TIME_ACCESS: u32 = 47;
    pub const TIME_ACCESS_SET: u32 = 48;
    pub const TIME_METADATA: u32 = 52;
    pub const TIME_MODIFY: u32 = 53;
    pub const TIME_MODIFY_SET: u32 = 54;
    pub const MOUNTED_ON_FILEID: u32 = 55;
    pub const CHANGE_POLICY: u32 = 60;
    pub const SUPPATTR_EXCLCREAT: u32 = 75;
}

/// `fh_expire_type` values (`FH4_*`).
pub mod expire_type {
    pub const PERSISTENT: u32 = 0x0;
    pub const NOEXPIRE_WITH_OPEN: u32 = 0x1;
    pub const VOLATILE_ANY: u32 = 0x2;
}

/// SEQUENCE status flags (`SEQ4_STATUS_*`); the names drop the shared prefix.
pub mod sequence_flag {
    /// SEQ4_STATUS_CB_PATH_DOWN: no back channel is bound.
    pub const PATH_DOWN: u32 = 0x1;
    /// SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED
    pub const EXPIRED_ALL: u32 = 0x8;
    /// SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED
    pub const EXPIRED_SOME: u32 = 0x10;
    /// SEQ4_STATUS_ADMIN_STATE_REVOKED
    pub const ADMIN_REVOKED: u32 = 0x20;
}

/// ACCESS bits (`ACCESS4_*`).
pub mod access {
    pub const READ: u32 = 0x01;
    pub const LOOKUP: u32 = 0x02;
    pub const MODIFY: u32 = 0x04;
    pub const EXTEND: u32 = 0x08;
    pub const DELETE: u32 = 0x10;
    pub const EXECUTE: u32 = 0x20;
    pub const ALL: u32 = 0x3f;
    /// The bits that change data or names, which a read-only export never grants.
    pub const MUTATING: u32 = MODIFY | EXTEND | DELETE;
}

/// OPEN share access and deny values (`OPEN4_SHARE_*`).
pub mod share {
    pub const READ: u32 = 1;
    pub const WRITE: u32 = 2;
    pub const BOTH: u32 = 3;
    /// The delegation wish bits a client may add to the access value.
    pub const WANT_MASK: u32 = 0xff00;
    pub const WANT_NONE: u32 = 0x0400;
    pub const WANT_CANCEL: u32 = 0x0500;
}

/// A `stateid4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Stateid {
    pub seqid: u32,
    pub other: [u8; 12],
}

/// The special stateids of RFC 8881 section 8.2.3.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpecialStateid {
    Anonymous,
    /// The READ bypass stateid of all ones.
    Bypass,
    Current,
    Invalid,
}

impl Stateid {
    pub const ANONYMOUS: Self = Self {
        seqid: 0,
        other: [0; 12],
    };
    pub const BYPASS: Self = Self {
        seqid: u32::MAX,
        other: [0xff; 12],
    };
    pub const CURRENT: Self = Self {
        seqid: 1,
        other: [0; 12],
    };
    pub const INVALID: Self = Self {
        seqid: u32::MAX,
        other: [0; 12],
    };

    /// The special meaning of this stateid; a zero or all-ones `other` with any other seqid is
    /// `Invalid`, which every operation refuses as BAD_STATEID.
    pub fn special(&self) -> Option<SpecialStateid> {
        if self.other == [0; 12] {
            return Some(match self.seqid {
                0 => SpecialStateid::Anonymous,
                1 => SpecialStateid::Current,
                _ => SpecialStateid::Invalid,
            });
        }
        if self.other == [0xff; 12] {
            return Some(match self.seqid {
                u32::MAX => SpecialStateid::Bypass,
                _ => SpecialStateid::Invalid,
            });
        }
        None
    }
}

/// Whether `bit` is set in `bitmap`.
pub fn bitmap_has(bitmap: &[u32], bit: u32) -> bool {
    let word = (bit / 32) as usize;
    bitmap
        .get(word)
        .is_some_and(|value| value & (1 << (bit % 32)) != 0)
}

/// Sets `bit` in `bitmap`, growing it as needed.
pub fn bitmap_set(bitmap: &mut Vec<u32>, bit: u32) {
    let word = (bit / 32) as usize;
    if bitmap.len() <= word {
        bitmap.resize(word + 1, 0);
    }
    bitmap[word] |= 1 << (bit % 32);
}

/// The set bits of `bitmap` in ascending order.
pub fn bitmap_bits(bitmap: &[u32]) -> impl Iterator<Item = u32> + '_ {
    bitmap.iter().enumerate().flat_map(|(word, value)| {
        (0..32u32)
            .filter(move |bit| value & (1 << bit) != 0)
            .map(move |bit| word as u32 * 32 + bit)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every operation number round-trips, and gaps or unknown numbers are illegal.
    #[test]
    fn operation_codes_roundtrip() {
        for code in 3..=58 {
            let operation = NfsOp4::from_code(code).expect("a defined operation");
            assert_eq!(operation.code(), code);
        }
        assert_eq!(NfsOp4::from_code(2), None);
        assert_eq!(NfsOp4::from_code(59), None);
        assert_eq!(NfsOp4::from_code(10044), None);
    }

    // The four special stateids and the invalid mixtures are told apart.
    #[test]
    fn special_stateids_classified() {
        assert_eq!(
            Stateid::ANONYMOUS.special(),
            Some(SpecialStateid::Anonymous)
        );
        assert_eq!(Stateid::BYPASS.special(), Some(SpecialStateid::Bypass));
        assert_eq!(Stateid::CURRENT.special(), Some(SpecialStateid::Current));
        assert_eq!(Stateid::INVALID.special(), Some(SpecialStateid::Invalid));
        let mixed = Stateid {
            seqid: 7,
            other: [0xff; 12],
        };
        assert_eq!(mixed.special(), Some(SpecialStateid::Invalid));
        let normal = Stateid {
            seqid: 1,
            other: [1; 12],
        };
        assert_eq!(normal.special(), None);
    }

    #[test]
    fn bitmap_bits_ascend() {
        let mut bitmap = Vec::new();
        bitmap_set(&mut bitmap, attribute::MOUNTED_ON_FILEID);
        bitmap_set(&mut bitmap, attribute::TYPE);
        assert!(bitmap_has(&bitmap, attribute::TYPE));
        assert!(!bitmap_has(&bitmap, attribute::SIZE));
        let bits: Vec<u32> = bitmap_bits(&bitmap).collect();
        assert_eq!(bits, vec![attribute::TYPE, attribute::MOUNTED_ON_FILEID]);
    }
}
