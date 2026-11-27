//! The `fattr4` codec: the supported attribute set, GETATTR and READDIR values, VERIFY
//! comparison and the settable attributes of SETATTR, CREATE and OPEN.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;

use crate::backend::{AttributeChange, ObjectAttributes, SetTime};
use crate::types::{
    IO_LIMIT, NAME_LIMIT, NfsStat4, NfsTime, attribute, bitmap_bits, bitmap_has, bitmap_set,
};
use crate::xdr::{XdrError, XdrReader, XdrWriter};

/// Most bitmap words a client may send.
const BITMAP_LIMIT: usize = 8;
/// Largest attribute value block a client may send, in bytes.
const VALUES_LIMIT: usize = 64 * 1024;
/// Longest owner string a client may send.
const OWNER_LIMIT: usize = 1024;

/// Every attribute this server reports or accepts, in ascending bit order.
const SUPPORTED: [u32; 32] = [
    attribute::SUPPORTED_ATTRS,
    attribute::TYPE,
    attribute::FH_EXPIRE_TYPE,
    attribute::CHANGE,
    attribute::SIZE,
    attribute::LINK_SUPPORT,
    attribute::SYMLINK_SUPPORT,
    attribute::NAMED_ATTR,
    attribute::FSID,
    attribute::UNIQUE_HANDLES,
    attribute::LEASE_TIME,
    attribute::RDATTR_ERROR,
    attribute::FILEHANDLE,
    attribute::FILEID,
    attribute::MAXFILESIZE,
    attribute::MAXNAME,
    attribute::MAXREAD,
    attribute::MAXWRITE,
    attribute::MODE,
    attribute::NUMLINKS,
    attribute::OWNER,
    attribute::OWNER_GROUP,
    attribute::RAWDEV,
    attribute::SPACE_USED,
    attribute::TIME_ACCESS,
    attribute::TIME_ACCESS_SET,
    attribute::TIME_METADATA,
    attribute::TIME_MODIFY,
    attribute::TIME_MODIFY_SET,
    attribute::MOUNTED_ON_FILEID,
    attribute::CHANGE_POLICY,
    attribute::SUPPATTR_EXCLCREAT,
];
const WRITE_ONLY: [u32; 2] = [attribute::TIME_ACCESS_SET, attribute::TIME_MODIFY_SET];
/// What EXCLUSIVE4_1 may set next to its verifier.
const EXCLUSIVE_SETTABLE: [u32; 2] = [attribute::SIZE, attribute::MODE];

fn bitmap_of(bits: &[u32]) -> Vec<u32> {
    let mut bitmap = Vec::new();
    for bit in bits {
        bitmap_set(&mut bitmap, *bit);
    }
    bitmap
}

pub fn supported_bitmap() -> Vec<u32> {
    bitmap_of(&SUPPORTED)
}

pub fn read_bitmap(reader: &mut XdrReader) -> Result<Vec<u32>, XdrError> {
    let count = reader.count(BITMAP_LIMIT)?;
    let mut bitmap = Vec::with_capacity(count);
    for _ in 0..count {
        bitmap.push(reader.u32()?);
    }
    Ok(bitmap)
}

pub fn write_bitmap(writer: &mut XdrWriter, bitmap: &[u32]) {
    let used = bitmap
        .iter()
        .rposition(|word| *word != 0)
        .map_or(0, |last| last + 1);
    writer.put_u32(used as u32);
    for word in &bitmap[..used] {
        writer.put_u32(*word);
    }
}

/// A client-sent `fattr4`: the mask and the still encoded values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientAttributes {
    pub mask: Vec<u32>,
    pub values: Bytes,
}

impl ClientAttributes {
    pub fn decode(reader: &mut XdrReader) -> Result<Self, XdrError> {
        let mask = read_bitmap(reader)?;
        let values = reader.opaque(VALUES_LIMIT)?;
        Ok(Self { mask, values })
    }
}

/// Everything an attribute encoding needs besides the object attributes.
pub struct AttributeView<'a> {
    pub attributes: &'a ObjectAttributes,
    pub handle: &'a [u8],
    pub fsid: (u64, u64),
    pub expire: u32,
    pub lease_secs: u32,
    pub mounted_on: u64,
}

/// Whether a GETATTR or READDIR request names a write-only attribute, which is INVAL.
pub fn requests_write_only(request: &[u32]) -> bool {
    WRITE_ONLY.iter().any(|bit| bitmap_has(request, *bit))
}

/// The supported, readable subset of `request` and its encoded values.
pub fn encode_values(request: &[u32], view: &AttributeView<'_>) -> (Vec<u32>, XdrWriter) {
    let mut mask = Vec::new();
    let mut values = XdrWriter::new();
    for bit in bitmap_bits(request) {
        if !SUPPORTED.contains(&bit) || WRITE_ONLY.contains(&bit) {
            continue;
        }
        bitmap_set(&mut mask, bit);
        put_value(&mut values, bit, view);
    }
    (mask, values)
}

/// Writes a complete `fattr4` for `request`.
pub fn write_fattr(out: &mut XdrWriter, request: &[u32], view: &AttributeView<'_>) {
    let (mask, values) = encode_values(request, view);
    write_bitmap(out, &mask);
    out.put_opaque(values.as_slice());
}

fn put_value(values: &mut XdrWriter, bit: u32, view: &AttributeView<'_>) {
    let attributes = view.attributes;
    match bit {
        attribute::SUPPORTED_ATTRS => write_bitmap(values, &supported_bitmap()),
        attribute::TYPE => values.put_u32(attributes.kind as u32),
        attribute::FH_EXPIRE_TYPE => values.put_u32(view.expire),
        attribute::CHANGE => values.put_u64(attributes.change),
        attribute::SIZE => values.put_u64(attributes.size),
        attribute::LINK_SUPPORT | attribute::SYMLINK_SUPPORT | attribute::NAMED_ATTR => {
            values.put_bool(false)
        }
        attribute::FSID => {
            values.put_u64(view.fsid.0);
            values.put_u64(view.fsid.1);
        }
        attribute::UNIQUE_HANDLES => values.put_bool(true),
        attribute::LEASE_TIME => values.put_u32(view.lease_secs),
        attribute::RDATTR_ERROR => values.put_u32(NfsStat4::Ok.code()),
        attribute::FILEHANDLE => values.put_opaque(view.handle),
        attribute::FILEID => values.put_u64(attributes.fileid),
        attribute::MAXFILESIZE => values.put_u64(i64::MAX as u64),
        attribute::MAXNAME => values.put_u32(NAME_LIMIT as u32),
        attribute::MAXREAD | attribute::MAXWRITE => values.put_u64(u64::from(IO_LIMIT)),
        attribute::MODE => values.put_u32(attributes.mode & 0o7777),
        attribute::NUMLINKS => values.put_u32(attributes.links),
        attribute::OWNER => values.put_string(&attributes.uid.to_string()),
        attribute::OWNER_GROUP => values.put_string(&attributes.gid.to_string()),
        attribute::RAWDEV => {
            values.put_u32(0);
            values.put_u32(0);
        }
        attribute::SPACE_USED => values.put_u64(attributes.space_used),
        attribute::TIME_ACCESS => put_time(values, attributes.accessed),
        attribute::TIME_METADATA => put_time(values, attributes.changed),
        attribute::TIME_MODIFY => put_time(values, attributes.modified),
        attribute::MOUNTED_ON_FILEID => values.put_u64(view.mounted_on),
        attribute::CHANGE_POLICY => {
            values.put_u64(1);
            values.put_u64(0);
        }
        attribute::SUPPATTR_EXCLCREAT => write_bitmap(values, &bitmap_of(&EXCLUSIVE_SETTABLE)),
        _ => {}
    }
}

fn put_time(values: &mut XdrWriter, time: NfsTime) {
    values.put_i64(time.seconds);
    values.put_u32(time.nanos);
}

fn read_time(reader: &mut XdrReader) -> Result<NfsTime, XdrError> {
    let seconds = reader.i64()?;
    let nanos = reader.u32()?;
    Ok(NfsTime { seconds, nanos })
}

fn read_settime(reader: &mut XdrReader) -> Result<SetTime, NfsStat4> {
    match reader.u32()? {
        0 => Ok(SetTime::Server),
        1 => {
            let time = read_time(reader)?;
            if time.nanos >= 1_000_000_000 {
                return Err(NfsStat4::Invalid);
            }
            Ok(SetTime::Client(time))
        }
        _ => Err(NfsStat4::BadXdr),
    }
}

/// The changes `attributes` asks for and the mask of attributes it sets. Owners never change
/// through NFS; unsupported bits are ATTRNOTSUPP and read-only ones INVAL.
pub fn decode_change(
    attributes: &ClientAttributes,
) -> Result<(AttributeChange, Vec<u32>), NfsStat4> {
    let mut change = AttributeChange::default();
    let mut reader = XdrReader::new(attributes.values.clone());
    for bit in bitmap_bits(&attributes.mask) {
        match bit {
            attribute::SIZE => change.size = Some(reader.u64()?),
            attribute::MODE => change.mode = Some(reader.u32()? & 0o7777),
            attribute::OWNER | attribute::OWNER_GROUP => {
                reader.string(OWNER_LIMIT)?;
                return Err(NfsStat4::NotOwner);
            }
            attribute::TIME_ACCESS_SET => change.accessed = Some(read_settime(&mut reader)?),
            attribute::TIME_MODIFY_SET => change.modified = Some(read_settime(&mut reader)?),
            other if SUPPORTED.contains(&other) => return Err(NfsStat4::Invalid),
            _ => return Err(NfsStat4::AttributeUnsupported),
        }
    }
    if reader.remaining() != 0 {
        return Err(NfsStat4::BadXdr);
    }
    Ok((change, attributes.mask.clone()))
}

/// Whether the mask of an EXCLUSIVE4_1 create stays inside SUPPATTR_EXCLCREAT.
pub fn exclusive_settable(mask: &[u32]) -> bool {
    bitmap_bits(mask).all(|bit| EXCLUSIVE_SETTABLE.contains(&bit))
}

/// VERIFY and NVERIFY: whether the client values equal the server's for the same mask.
pub fn same_values(
    attributes: &ClientAttributes,
    view: &AttributeView<'_>,
) -> Result<bool, NfsStat4> {
    for bit in bitmap_bits(&attributes.mask) {
        if !SUPPORTED.contains(&bit) {
            return Err(NfsStat4::AttributeUnsupported);
        }
        if bit == attribute::RDATTR_ERROR || WRITE_ONLY.contains(&bit) {
            return Err(NfsStat4::Invalid);
        }
    }
    let (_, values) = encode_values(&attributes.mask, view);
    Ok(values.as_slice() == &attributes.values[..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NfsType4;

    fn sample() -> ObjectAttributes {
        ObjectAttributes {
            kind: NfsType4::Regular,
            size: 5,
            change: 9,
            fileid: 77,
            mode: 0o100644,
            links: 1,
            uid: 1000,
            gid: 1000,
            space_used: 5,
            accessed: NfsTime::default(),
            modified: NfsTime::default(),
            changed: NfsTime::default(),
        }
    }

    fn view(attributes: &ObjectAttributes) -> AttributeView<'_> {
        AttributeView {
            attributes,
            handle: &[1, 2, 3],
            fsid: (4, 5),
            expire: 0,
            lease_secs: 90,
            mounted_on: 77,
        }
    }

    // The reply mask drops unsupported and write-only bits and keeps ascending value order.
    #[test]
    fn encodes_requested_subset() {
        let attributes = sample();
        let request = bitmap_of(&[
            attribute::SIZE,
            attribute::TYPE,
            12,
            attribute::TIME_ACCESS_SET,
        ]);
        let (mask, values) = encode_values(&request, &view(&attributes));
        assert_eq!(mask, bitmap_of(&[attribute::TYPE, attribute::SIZE]));
        let mut reader = XdrReader::new(values.into_bytes());
        assert_eq!(reader.u32(), Ok(NfsType4::Regular as u32));
        assert_eq!(reader.u64(), Ok(5));
        assert_eq!(reader.remaining(), 0);
        assert!(requests_write_only(&request));
    }

    #[test]
    fn verify_compares_values() {
        let attributes = sample();
        let mask = bitmap_of(&[attribute::SIZE]);
        let (_, values) = encode_values(&mask, &view(&attributes));
        let same = ClientAttributes {
            mask: mask.clone(),
            values: values.into_bytes(),
        };
        assert_eq!(same_values(&same, &view(&attributes)), Ok(true));
        let mut other = XdrWriter::new();
        other.put_u64(6);
        let differs = ClientAttributes {
            mask,
            values: other.into_bytes(),
        };
        assert_eq!(same_values(&differs, &view(&attributes)), Ok(false));
        let error = ClientAttributes {
            mask: bitmap_of(&[attribute::RDATTR_ERROR]),
            values: Bytes::new(),
        };
        assert_eq!(
            same_values(&error, &view(&attributes)),
            Err(NfsStat4::Invalid)
        );
    }

    #[test]
    fn decodes_settable_change() {
        let mut values = XdrWriter::new();
        values.put_u64(0);
        values.put_u32(0o100755);
        values.put_u32(0);
        let attributes = ClientAttributes {
            mask: bitmap_of(&[attribute::SIZE, attribute::MODE, attribute::TIME_MODIFY_SET]),
            values: values.into_bytes(),
        };
        let (change, mask) = decode_change(&attributes).expect("change");
        assert_eq!(change.size, Some(0));
        assert_eq!(change.mode, Some(0o755));
        assert_eq!(change.modified, Some(SetTime::Server));
        assert_eq!(mask, attributes.mask);
        let readonly = ClientAttributes {
            mask: bitmap_of(&[attribute::FILEID]),
            values: Bytes::from_static(&[0; 8]),
        };
        assert_eq!(decode_change(&readonly), Err(NfsStat4::Invalid));
        let unknown = ClientAttributes {
            mask: bitmap_of(&[12]),
            values: Bytes::new(),
        };
        assert_eq!(decode_change(&unknown), Err(NfsStat4::AttributeUnsupported));
    }
}
