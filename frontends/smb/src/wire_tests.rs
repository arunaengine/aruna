//! Unit tests for SMB2 framing, headers, transform headers and compound splitting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use tokio::io::AsyncWriteExt;

fn element(command: u16, message_id: u64, body: &[u8]) -> Vec<u8> {
    let header = Header {
        command,
        message_id,
        credits: 1,
        ..Header::default()
    };
    let mut out = Vec::new();
    header.encode(&mut out);
    out.extend_from_slice(body);
    out
}

#[test]
fn header_round_trip() {
    let sync = Header {
        credit_charge: 2,
        status: 0xC000_0022,
        command: 5,
        credits: 31,
        flags: FLAG_RESPONSE | FLAG_RELATED,
        next_command: 0,
        message_id: 77,
        async_id: 0,
        tree_id: 9,
        session_id: 0x1122_3344_5566_7788,
        signature: [3u8; 16],
    };
    let mut bytes = Vec::new();
    sync.encode(&mut bytes);
    assert_eq!(bytes.len(), HEADER_SIZE);
    assert_eq!(Header::decode(&bytes), Ok(sync.clone()));
    let pending = Header {
        flags: FLAG_RESPONSE | FLAG_ASYNC,
        async_id: 0xAB,
        tree_id: 0,
        ..sync
    };
    let mut bytes = Vec::new();
    pending.encode(&mut bytes);
    assert_eq!(&bytes[32..40], &0xABu64.to_le_bytes());
    assert_eq!(Header::decode(&bytes), Ok(pending));
}

#[test]
fn header_rejects_garbage() {
    assert_eq!(Header::decode(&[0u8; 10]), Err(WireError::Truncated));
    let mut bytes = Vec::new();
    Header::default().encode(&mut bytes);
    bytes[0] = 0xFF;
    assert!(Header::decode(&bytes).is_err());
    let mut bytes = Vec::new();
    Header::default().encode(&mut bytes);
    bytes[4] = 63;
    assert!(Header::decode(&bytes).is_err());
}

#[test]
fn compound_round_trip() {
    let first = element(5, 1, &[1u8; 57]);
    let second = element(16, 2, &[2u8; 41]);
    let third = element(6, 3, &[3u8; 24]);
    let mut joined = join_compound(vec![first.clone(), second.clone(), third.clone()]);
    let parts = split_compound(&joined).expect("compound splits");
    assert_eq!(parts.len(), 3);
    let padded = first.len().next_multiple_of(8);
    assert_eq!(parts[0].len(), padded);
    assert_eq!(
        Header::decode(parts[0]).expect("header").next_command as usize,
        padded
    );
    assert_eq!(&parts[0][HEADER_SIZE..first.len()], &first[HEADER_SIZE..]);
    assert_eq!(parts[1].len(), second.len().next_multiple_of(8));
    assert_eq!(Header::decode(parts[1]).expect("header").message_id, 2);
    assert_eq!(Header::decode(parts[2]).expect("header").next_command, 0);
    assert_eq!(parts[2], &third[..]);
    joined[20..24].copy_from_slice(&13u32.to_le_bytes());
    assert!(split_compound(&joined).is_err(), "misaligned NextCommand");
    joined[20..24].copy_from_slice(&32u32.to_le_bytes());
    assert!(
        split_compound(&joined).is_err(),
        "NextCommand inside the header"
    );
    joined[20..24].copy_from_slice(&4096u32.to_le_bytes());
    assert!(split_compound(&joined).is_err(), "NextCommand past the end");
}

#[test]
fn buffers_stay_bounded() {
    let message = vec![0u8; 100];
    assert_eq!(buffer_at(&message, 64, 36).map(<[u8]>::len), Ok(36));
    assert!(buffer_at(&message, 64, 37).is_err());
    assert!(buffer_at(&message, 10, 4).is_err());
    assert!(buffer_at(&message, usize::MAX, 4).is_err());
    assert_eq!(buffer_at(&message, 0, 0), Ok(&[][..]));
}

#[test]
fn utf16_rejects_odd() {
    assert_eq!(utf16_text(&utf16_bytes("Grüße")), Some("Grüße".to_owned()));
    assert_eq!(utf16_text(&[0x41]), None);
    assert_eq!(utf16_text(&[0x00, 0xD8]), None);
}

#[test]
fn transform_associated_data() {
    let header = TransformHeader {
        signature: [9u8; 16],
        nonce: [7u8; 16],
        message_size: 300,
        flags: TransformHeader::ENCRYPTED,
        session_id: 42,
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    assert_eq!(bytes.len(), TRANSFORM_SIZE);
    assert_eq!(TransformHeader::decode(&bytes), Ok(header.clone()));
    let associated = header.associated();
    assert_eq!(associated.len(), 32);
    assert_eq!(&associated[..16], &[7u8; 16]);
    assert_eq!(&associated[24..], &42u64.to_le_bytes());
}

#[tokio::test]
async fn limit_precedes_allocation() {
    let (mut client, mut server) = tokio::io::duplex(64);
    client
        .write_all(&[0, 0xFF, 0xFF, 0xFF])
        .await
        .expect("prefix written");
    assert_eq!(
        read_frame(&mut server, 1024).await,
        Err(WireError::TooLarge(0x00FF_FFFF))
    );
}

#[tokio::test]
async fn frame_round_trip() {
    let (mut client, mut server) = tokio::io::duplex(1024);
    let message = element(13, 4, &[4, 0, 0, 0]);
    write_frame(&mut client, &frame(&message).expect("frame"))
        .await
        .expect("written");
    assert_eq!(read_frame(&mut server, 1024).await, Ok(Some(message)));
    drop(client);
    assert_eq!(read_frame(&mut server, 1024).await, Ok(None));
}

#[tokio::test]
async fn frame_type_zero() {
    let (mut client, mut server) = tokio::io::duplex(64);
    client
        .write_all(&[0x85, 0, 0, 0])
        .await
        .expect("prefix written");
    assert!(read_frame(&mut server, 1024).await.is_err());
}
