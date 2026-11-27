//! NTSTATUS codes the SMB server returns, named without the `STATUS_` prefix and shortened to
//! three terms where the protocol name is longer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub const SUCCESS: u32 = 0x0000_0000;
pub const PENDING: u32 = 0x0000_0103;
pub const BUFFER_OVERFLOW: u32 = 0x8000_0005;
pub const NO_MORE_FILES: u32 = 0x8000_0006;
pub const NOT_IMPLEMENTED: u32 = 0xC000_0002;
pub const INVALID_INFO_CLASS: u32 = 0xC000_0003;
pub const INFO_LENGTH_MISMATCH: u32 = 0xC000_0004;
pub const INVALID_HANDLE: u32 = 0xC000_0008;
pub const INVALID_PARAMETER: u32 = 0xC000_000D;
pub const NO_SUCH_FILE: u32 = 0xC000_000F;
pub const INVALID_DEVICE_REQUEST: u32 = 0xC000_0010;
pub const END_OF_FILE: u32 = 0xC000_0011;
/// `STATUS_MORE_PROCESSING_REQUIRED`.
pub const MORE_PROCESSING: u32 = 0xC000_0016;
pub const ACCESS_DENIED: u32 = 0xC000_0022;
pub const BUFFER_TOO_SMALL: u32 = 0xC000_0023;
/// `STATUS_OBJECT_NAME_INVALID`.
pub const NAME_INVALID: u32 = 0xC000_0033;
/// `STATUS_OBJECT_NAME_NOT_FOUND`.
pub const NAME_NOT_FOUND: u32 = 0xC000_0034;
/// `STATUS_OBJECT_NAME_COLLISION`.
pub const NAME_COLLISION: u32 = 0xC000_0035;
/// `STATUS_OBJECT_PATH_NOT_FOUND`.
pub const PATH_NOT_FOUND: u32 = 0xC000_003A;
pub const SHARING_VIOLATION: u32 = 0xC000_0043;
pub const QUOTA_EXCEEDED: u32 = 0xC000_0044;
/// `STATUS_FILE_LOCK_CONFLICT`.
pub const LOCK_CONFLICT: u32 = 0xC000_0054;
pub const LOCK_NOT_GRANTED: u32 = 0xC000_0055;
pub const DELETE_PENDING: u32 = 0xC000_0056;
/// `STATUS_NO_EAS_ON_FILE`: the file carries no extended attributes.
pub const NO_EXTENDED: u32 = 0xC000_0052;
pub const PRIVILEGE_NOT_HELD: u32 = 0xC000_0061;
pub const LOGON_FAILURE: u32 = 0xC000_006D;
pub const RANGE_NOT_LOCKED: u32 = 0xC000_007E;
pub const DISK_FULL: u32 = 0xC000_007F;
pub const INSUFFICIENT_RESOURCES: u32 = 0xC000_009A;
pub const MEDIA_WRITE_PROTECTED: u32 = 0xC000_00A2;
/// `STATUS_FILE_IS_A_DIRECTORY`.
pub const IS_DIRECTORY: u32 = 0xC000_00BA;
pub const NOT_SUPPORTED: u32 = 0xC000_00BB;
pub const NETWORK_NAME_DELETED: u32 = 0xC000_00C9;
pub const BAD_NETWORK_NAME: u32 = 0xC000_00CC;
pub const REQUEST_NOT_ACCEPTED: u32 = 0xC000_00D0;
pub const NOT_SAME_DEVICE: u32 = 0xC000_00D4;
pub const INTERNAL_ERROR: u32 = 0xC000_00E5;
pub const DIRECTORY_NOT_EMPTY: u32 = 0xC000_0101;
pub const NOT_DIRECTORY: u32 = 0xC000_0103;
pub const CANCELLED: u32 = 0xC000_0120;
pub const CANNOT_DELETE: u32 = 0xC000_0121;
pub const FILE_CLOSED: u32 = 0xC000_0128;
pub const FS_DRIVER_REQUIRED: u32 = 0xC000_019C;
pub const INVALID_LOCK_RANGE: u32 = 0xC000_01A1;
pub const USER_SESSION_DELETED: u32 = 0xC000_0203;
pub const NOT_FOUND: u32 = 0xC000_0225;
pub const NETWORK_SESSION_EXPIRED: u32 = 0xC000_035C;

/// Whether a status reports failure (severity error) rather than success or a warning.
pub fn is_error(status: u32) -> bool {
    status >> 30 == 0b11
}
