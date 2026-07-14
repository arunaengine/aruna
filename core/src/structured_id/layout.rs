//! Sole owner of the Aruna Structured ULID bit layout (Appendix A.1):
//! `timestamp_ms(48) | placement_handle(20) | bucket(12) | nonce(48)`.
//! No other module performs raw field shifts or masks.

pub const TIMESTAMP_BITS: u32 = 48;
pub const HANDLE_BITS: u32 = 20;
pub const BUCKET_BITS: u32 = 12;
pub const NONCE_BITS: u32 = 48;

pub const BUCKET_SHIFT: u32 = NONCE_BITS;
pub const HANDLE_SHIFT: u32 = NONCE_BITS + BUCKET_BITS;
pub const TIMESTAMP_SHIFT: u32 = NONCE_BITS + BUCKET_BITS + HANDLE_BITS;

pub const HANDLE_MASK: u32 = (1u32 << HANDLE_BITS) - 1;
pub const BUCKET_MASK: u16 = (1u16 << BUCKET_BITS) - 1;
pub const TIMESTAMP_MASK: u64 = (1u64 << TIMESTAMP_BITS) - 1;
pub const NONCE_MASK: u64 = (1u64 << NONCE_BITS) - 1;

pub const MAX_HANDLE: u32 = HANDLE_MASK;
pub const MAX_BUCKET: u16 = BUCKET_MASK;
pub const MAX_TIMESTAMP_MS: u64 = TIMESTAMP_MASK;
pub const MAX_NONCE: u64 = NONCE_MASK;

/// Handle zero is reserved and MUST NOT be allocated (REQ-META-ID-FORMAT-001).
pub const RESERVED_HANDLE: u32 = 0;
/// The 12-bit bucket field spans the maximum `bucket_count` (REQ-META-ID-FORMAT-001).
pub const MAX_BUCKET_COUNT: u16 = 1u16 << BUCKET_BITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fields {
    pub timestamp_ms: u64,
    pub handle: u32,
    pub bucket: u16,
    pub nonce: u64,
}

pub const fn pack(timestamp_ms: u64, handle: u32, bucket: u16, nonce: u64) -> u128 {
    (((timestamp_ms & TIMESTAMP_MASK) as u128) << TIMESTAMP_SHIFT)
        | (((handle & HANDLE_MASK) as u128) << HANDLE_SHIFT)
        | (((bucket & BUCKET_MASK) as u128) << BUCKET_SHIFT)
        | ((nonce & NONCE_MASK) as u128)
}

pub const fn unpack(value: u128) -> Fields {
    Fields {
        timestamp_ms: (value >> TIMESTAMP_SHIFT) as u64,
        handle: ((value >> HANDLE_SHIFT) as u32) & HANDLE_MASK,
        bucket: ((value >> BUCKET_SHIFT) as u16) & BUCKET_MASK,
        nonce: (value as u64) & NONCE_MASK,
    }
}
