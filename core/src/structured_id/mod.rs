//! Aruna Structured ULID codec (spec Appendix A.1, section 6.3.4).
//!
//! A `MetaResourceId`/`JobId` is a 26-character Crockford Base32 ULID whose
//! 80-bit entropy field is partitioned into a 20-bit placement handle, a 12-bit
//! bucket, and a 48-bit nonce. All raw bit knowledge lives in [`layout`]; this
//! module exposes only typed fields so downstream code never touches raw bits.

mod generator;
mod layout;

pub use generator::{
    ClockHealthError, DEFAULT_MAX_ID_CLOCK_SKEW_MS, IdEnvironment, StructuredIdGenerator,
    SystemEnvironment,
};

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::{DecodeError, Ulid};

/// Highest allocatable placement handle; handle zero is reserved.
pub const MAX_PLACEMENT_HANDLE: u32 = layout::MAX_HANDLE;
/// Highest bucket value the 12-bit field can hold.
pub const MAX_BUCKET_ID: u16 = layout::MAX_BUCKET;
/// Maximum `bucket_count` a strategy may declare (the 12-bit field cap).
pub const MAX_BUCKET_COUNT: u16 = layout::MAX_BUCKET_COUNT;
/// Number of allocatable handles (20 bits, handle zero reserved).
pub const ALLOCATABLE_HANDLES: u32 = layout::MAX_HANDLE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FieldError {
    #[error("placement handle 0 is reserved and must not be allocated")]
    ReservedHandle,
    #[error("placement handle {0} exceeds the 20-bit range")]
    HandleOutOfRange(u32),
    #[error("bucket {0} exceeds the 12-bit range")]
    BucketOutOfRange(u16),
    #[error("timestamp {0} exceeds the 48-bit range")]
    TimestampOutOfRange(u64),
    #[error("nonce {0} exceeds the 48-bit range")]
    NonceOutOfRange(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ParseError {
    #[error("structured id must be 26 characters")]
    InvalidLength,
    #[error("invalid Crockford base32 character")]
    InvalidChar,
    #[error("first character exceeds 7: value does not fit 128 bits")]
    Overflow,
    #[error("placement handle 0 is reserved and must not be allocated")]
    ReservedHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("bucket {bucket} is not less than bucket_count {bucket_count}")]
pub struct BucketNotInRange {
    pub bucket: u16,
    pub bucket_count: u16,
}

/// A non-zero realm-local 20-bit placement handle (REQ-META-ID-FORMAT-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementHandle(u32);

impl PlacementHandle {
    pub const fn new(value: u32) -> Result<Self, FieldError> {
        if value == layout::RESERVED_HANDLE {
            Err(FieldError::ReservedHandle)
        } else if value > layout::MAX_HANDLE {
            Err(FieldError::HandleOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Serialize for PlacementHandle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for PlacementHandle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u32::deserialize(deserializer)?;
        PlacementHandle::new(value).map_err(serde::de::Error::custom)
    }
}

/// A 12-bit bucket carried inside the id (REQ-META-ID-FORMAT-001).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketId(u16);

impl BucketId {
    pub const fn new(value: u16) -> Result<Self, FieldError> {
        if value > layout::MAX_BUCKET {
            Err(FieldError::BucketOutOfRange(value))
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u16 {
        self.0
    }

    /// Fail-closed `bucket < bucket_count` check (REQ-META-ID-FORMAT-001): an id
    /// whose bucket reaches the strategy's `bucket_count` is invalid.
    pub const fn in_strategy_range(self, bucket_count: u16) -> Result<(), BucketNotInRange> {
        if bucket_count <= layout::MAX_BUCKET_COUNT && self.0 < bucket_count {
            Ok(())
        } else {
            Err(BucketNotInRange {
                bucket: self.0,
                bucket_count,
            })
        }
    }
}

fn decode_canonical(input: &str) -> Result<u128, ParseError> {
    let value = Ulid::from_string(input).map_err(|error| match error {
        DecodeError::InvalidLength => ParseError::InvalidLength,
        DecodeError::InvalidChar => ParseError::InvalidChar,
    })?;
    // `from_string` silently truncates a first character whose value exceeds 7;
    // a canonical 128-bit encoding always begins with a character in 0..=7.
    if !matches!(input.as_bytes()[0], b'0'..=b'7') {
        return Err(ParseError::Overflow);
    }
    Ok(value.0)
}

mod sealed {
    use ulid::Ulid;

    pub struct Token(());

    pub(super) fn new() -> Token {
        Token(())
    }

    /// The raw constructor lives here so no code outside this module can
    /// wrap an unvalidated ULID (e.g. one carrying the reserved handle 0).
    pub trait Sealed {
        fn from_ulid(ulid: Ulid, _token: Token) -> Self;
    }
}

/// Shared codec for the Aruna Structured ULID family (`MetaResourceId`,
/// `JobId`). Every constructor guarantees a non-zero handle, so extracted
/// fields are always valid.
pub trait StructuredId: Sized + Copy + sealed::Sealed {
    fn as_ulid(&self) -> Ulid;

    /// Builds an id from its four fields, rejecting an out-of-range timestamp or
    /// nonce. The handle and bucket are already range-checked newtypes.
    fn from_parts(
        timestamp_ms: u64,
        handle: PlacementHandle,
        bucket: BucketId,
        nonce: u64,
    ) -> Result<Self, FieldError> {
        if timestamp_ms > layout::MAX_TIMESTAMP_MS {
            return Err(FieldError::TimestampOutOfRange(timestamp_ms));
        }
        if nonce > layout::MAX_NONCE {
            return Err(FieldError::NonceOutOfRange(nonce));
        }
        Ok(Self::from_ulid(
            Ulid(layout::pack(
                timestamp_ms,
                handle.get(),
                bucket.get(),
                nonce,
            )),
            sealed::new(),
        ))
    }

    /// Parses the canonical 26-character form, normalizing lowercase and
    /// rejecting overflow, excluded characters, and the reserved handle.
    fn parse(input: &str) -> Result<Self, ParseError> {
        let value = decode_canonical(input)?;
        if layout::unpack(value).handle == layout::RESERVED_HANDLE {
            return Err(ParseError::ReservedHandle);
        }
        Ok(Self::from_ulid(Ulid(value), sealed::new()))
    }

    fn timestamp_ms(&self) -> u64 {
        layout::unpack(self.as_ulid().0).timestamp_ms
    }

    fn placement_handle(&self) -> PlacementHandle {
        PlacementHandle(layout::unpack(self.as_ulid().0).handle)
    }

    fn bucket(&self) -> BucketId {
        BucketId(layout::unpack(self.as_ulid().0).bucket)
    }

    fn nonce(&self) -> u64 {
        layout::unpack(self.as_ulid().0).nonce
    }

    fn as_u128(&self) -> u128 {
        self.as_ulid().0
    }

    fn to_bytes(&self) -> [u8; 16] {
        self.as_ulid().to_bytes()
    }

    /// Fail-closed `bucket < bucket_count` check for this id.
    fn validate_bucket(&self, bucket_count: u16) -> Result<(), BucketNotInRange> {
        self.bucket().in_strategy_range(bucket_count)
    }
}

macro_rules! structured_id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Ulid);

        impl sealed::Sealed for $name {
            fn from_ulid(ulid: Ulid, _token: sealed::Token) -> Self {
                Self(ulid)
            }
        }

        impl StructuredId for $name {
            fn as_ulid(&self) -> Ulid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                <$name as StructuredId>::parse(input)
            }
        }

        impl TryFrom<u128> for $name {
            type Error = FieldError;

            fn try_from(value: u128) -> Result<Self, Self::Error> {
                if layout::unpack(value).handle == layout::RESERVED_HANDLE {
                    return Err(FieldError::ReservedHandle);
                }
                Ok(Self(Ulid(value)))
            }
        }

        impl From<$name> for u128 {
            fn from(id: $name) -> u128 {
                id.0.0
            }
        }

        impl From<$name> for Ulid {
            fn from(id: $name) -> Ulid {
                id.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                let raw = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
                <$name as StructuredId>::parse(&raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

structured_id_newtype!(
    /// The stable 26-character placement-aware identity of a Meta Resource.
    MetaResourceId
);

structured_id_newtype!(
    /// The placement-aware identity of a durable job record (REQ-JOB-ID-FORMAT-001).
    JobId
);

#[cfg(test)]
mod tests {
    use super::layout;
    use super::*;
    use proptest::prelude::*;

    const KAT_STRING: &str = "014D2PF2DB1FQF0AG14D2PF2DB";
    const KAT_U128: u128 = 0x0123456789ab0beef02a0123456789ab;

    #[test]
    fn known_answer_vector() {
        // Appendix A.1 normative test vector: all six values must reproduce.
        let handle = PlacementHandle::new(0x0beef).unwrap();
        let bucket = BucketId::new(0x02a).unwrap();
        let id =
            MetaResourceId::from_parts(0x0123456789ab, handle, bucket, 0x0123456789ab).unwrap();
        assert_eq!(id.as_u128(), KAT_U128);
        assert_eq!(id.to_string(), KAT_STRING);

        let parsed = MetaResourceId::parse(KAT_STRING).unwrap();
        assert_eq!(parsed, id);
        assert_eq!(parsed.timestamp_ms(), 0x0123456789ab);
        assert_eq!(parsed.placement_handle().get(), 0x0beef);
        assert_eq!(parsed.bucket().get(), 0x02a);
        assert_eq!(parsed.nonce(), 0x0123456789ab);
    }

    proptest! {
        #[test]
        fn round_trip_random(
            timestamp_ms in 0u64..=layout::MAX_TIMESTAMP_MS,
            handle in 1u32..=layout::MAX_HANDLE,
            bucket in 0u16..=layout::MAX_BUCKET,
            nonce in 0u64..=layout::MAX_NONCE,
        ) {
            let id = MetaResourceId::from_parts(
                timestamp_ms,
                PlacementHandle::new(handle).unwrap(),
                BucketId::new(bucket).unwrap(),
                nonce,
            ).unwrap();
            let text = id.to_string();
            prop_assert_eq!(text.len(), 26);
            let parsed = MetaResourceId::parse(&text).unwrap();
            prop_assert_eq!(parsed, id);
            prop_assert_eq!(parsed.timestamp_ms(), timestamp_ms);
            prop_assert_eq!(parsed.placement_handle().get(), handle);
            prop_assert_eq!(parsed.bucket().get(), bucket);
            prop_assert_eq!(parsed.nonce(), nonce);
        }
    }

    #[test]
    fn handle_zero_rejected() {
        assert_eq!(PlacementHandle::new(0), Err(FieldError::ReservedHandle));
        let zero_handle = layout::pack(1, 0, 5, 9);
        assert_eq!(
            MetaResourceId::try_from(zero_handle),
            Err(FieldError::ReservedHandle)
        );
        let text = Ulid(zero_handle).to_string();
        assert_eq!(
            MetaResourceId::parse(&text),
            Err(ParseError::ReservedHandle)
        );
    }

    #[test]
    fn width_overflow_rejected() {
        assert_eq!(
            PlacementHandle::new(0x100000),
            Err(FieldError::HandleOutOfRange(0x100000))
        );
        assert_eq!(
            BucketId::new(0x1000),
            Err(FieldError::BucketOutOfRange(0x1000))
        );
    }

    #[test]
    fn bucket_count_check() {
        assert!(BucketId::new(63).unwrap().in_strategy_range(64).is_ok());
        assert_eq!(
            BucketId::new(64).unwrap().in_strategy_range(64),
            Err(BucketNotInRange {
                bucket: 64,
                bucket_count: 64,
            })
        );
        assert!(BucketId::new(0).unwrap().in_strategy_range(0).is_err());
        assert!(BucketId::new(4095).unwrap().in_strategy_range(4096).is_ok());
        assert!(
            BucketId::new(4095)
                .unwrap()
                .in_strategy_range(4097)
                .is_err()
        );
    }

    #[test]
    fn generic_ulid_distinguishable() {
        // A structured id always carries a non-zero handle and round-trips.
        let structured = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(42).unwrap(),
            BucketId::new(3).unwrap(),
            9,
        )
        .unwrap();
        assert_ne!(structured.placement_handle().get(), 0);
        assert!(MetaResourceId::parse(&structured.to_string()).is_ok());

        // A generic ULID does not preserve the structured fields: a zero handle
        // is rejected on parse, and a bucket outside a small bucket_count is
        // flagged by validation.
        let bucket_count = 8u16;
        let mut flagged = 0;
        for _ in 0..512 {
            let generic = Ulid::r#gen();
            let fields = layout::unpack(generic.0);
            match MetaResourceId::parse(&generic.to_string()) {
                Err(ParseError::ReservedHandle) => assert_eq!(fields.handle, 0),
                Ok(parsed) => {
                    assert_ne!(fields.handle, 0);
                    if fields.bucket >= bucket_count {
                        assert!(parsed.validate_bucket(bucket_count).is_err());
                        flagged += 1;
                    }
                }
                Err(other) => panic!("unexpected parse error: {other:?}"),
            }
        }
        assert!(flagged > 0);
    }

    #[test]
    fn lowercase_normalized() {
        let parsed = MetaResourceId::parse(&KAT_STRING.to_ascii_lowercase()).unwrap();
        assert_eq!(parsed.as_u128(), KAT_U128);
    }

    #[test]
    fn overflow_first_char() {
        let mut mutated = KAT_STRING.to_string();
        mutated.replace_range(0..1, "8");
        assert_eq!(MetaResourceId::parse(&mutated), Err(ParseError::Overflow));
        assert_eq!(
            MetaResourceId::parse(&"Z".repeat(26)),
            Err(ParseError::Overflow)
        );
    }

    #[test]
    fn excluded_chars_rejected() {
        for bad in ['I', 'L', 'O', 'U'] {
            let mut mutated = KAT_STRING.to_string();
            mutated.replace_range(5..6, &bad.to_string());
            assert_eq!(
                MetaResourceId::parse(&mutated),
                Err(ParseError::InvalidChar)
            );
        }
        assert_eq!(
            MetaResourceId::parse("TOO-SHORT"),
            Err(ParseError::InvalidLength)
        );
    }

    #[test]
    fn serde_round_trip() {
        let id = MetaResourceId::parse(KAT_STRING).unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{KAT_STRING}\""));
        let back: MetaResourceId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);

        let zero = Ulid(layout::pack(1, 0, 1, 1)).to_string();
        assert!(serde_json::from_str::<MetaResourceId>(&format!("\"{zero}\"")).is_err());
    }

    #[test]
    fn job_id_shares_codec() {
        let job = JobId::from_parts(
            0x0123456789ab,
            PlacementHandle::new(0x0beef).unwrap(),
            BucketId::new(0x02a).unwrap(),
            0x0123456789ab,
        )
        .unwrap();
        assert_eq!(job.to_string(), KAT_STRING);
        assert_eq!(JobId::parse(KAT_STRING).unwrap(), job);
        assert_eq!(job.placement_handle().get(), 0x0beef);
    }
}
