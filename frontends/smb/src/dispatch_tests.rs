//! Wire tests of the command handlers: CREATE grants, the per-command and per-class rights
//! matrix, append-only writes, directory enumeration and the rename buffer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::access::{
    ACCESS_SYSTEM_SECURITY, DELETE, END_OFFSET, FILE_ADD_FILE, FILE_APPEND_DATA,
    FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_READ_EA, FILE_WRITE_ATTRIBUTES,
    FILE_WRITE_DATA, GENERIC_READ, GENERIC_WRITE, MAXIMUM_ALLOWED, READ_CONTROL, READ_MAPPING,
};
use crate::info::security as parts;
use crate::info::volume;
use crate::testing::{
    Response, TestClient, directory_body, flush_body, logged_in, query_body, read_body,
    rename_buffer, set_body, write_body,
};
use crate::wire::utf16_bytes;

const OPEN: u32 = 1;
const CREATE: u32 = 2;
const LIMIT: u32 = 65_536;
const READER: Rights = Rights {
    read: true,
    write: false,
    delete: false,
};

async fn open(client: &mut TestClient, name: &str, desired: u32) -> FileId {
    let response = client.create(name, desired, OPEN, 0).await;
    assert_eq!(response.status(), status::SUCCESS, "open {name}");
    response.file_id()
}

async fn open_directory(client: &mut TestClient, name: &str, desired: u32) -> FileId {
    let response = client.create(name, desired, OPEN, DIRECTORY_FILE).await;
    assert_eq!(response.status(), status::SUCCESS, "open directory {name}");
    response.file_id()
}

async fn query(
    client: &mut TestClient,
    info_type: u8,
    file_class: u8,
    additional: u32,
    file_id: FileId,
) -> Response {
    query_sized(client, info_type, file_class, additional, file_id, LIMIT).await
}

async fn query_sized(
    client: &mut TestClient,
    info_type: u8,
    file_class: u8,
    additional: u32,
    file_id: FileId,
    limit: u32,
) -> Response {
    client
        .call(
            Command::QueryInfo,
            &query_body(info_type, file_class, additional, file_id, limit),
        )
        .await
}

async fn set(client: &mut TestClient, file_class: u8, file_id: FileId, buffer: &[u8]) -> Response {
    client
        .call(
            Command::SetInfo,
            &set_body(INFO_FILE, file_class, file_id, buffer),
        )
        .await
}

async fn write(client: &mut TestClient, file_id: FileId, offset: u64, data: &[u8]) -> Response {
    client
        .call(Command::Write, &write_body(file_id, offset, data))
        .await
}

fn written(response: &Response) -> u32 {
    let body = response.body();
    u32::from_le_bytes([body[4], body[5], body[6], body[7]])
}

/// Names, FileIndex and FileId of FileIdBothDirectoryInformation entries.
fn entries(buffer: &[u8]) -> Vec<(String, u32, u64)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    loop {
        let entry = &buffer[start..];
        let next = u32::from_le_bytes(entry[0..4].try_into().expect("offset")) as usize;
        let index = u32::from_le_bytes(entry[4..8].try_into().expect("index"));
        let length = u32::from_le_bytes(entry[60..64].try_into().expect("length")) as usize;
        let file_id = u64::from_le_bytes(entry[96..104].try_into().expect("file id"));
        let name = utf16_text(&entry[104..104 + length]).expect("name");
        out.push((name, index, file_id));
        if next == 0 {
            return out;
        }
        assert!(next.is_multiple_of(8), "entries are eight-byte aligned");
        start += next;
    }
}

fn names(buffer: &[u8]) -> Vec<String> {
    entries(buffer)
        .into_iter()
        .map(|(name, _, _)| name)
        .collect()
}

#[tokio::test]
async fn create_expands_generic() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("r.txt", ObjectKind::File, b"data");
    backend.state.lock().rights = READER;
    let file_id = open(&mut client, "r.txt", GENERIC_READ).await;
    let granted = query(&mut client, INFO_FILE, class::ACCESS, 0, file_id).await;
    assert_eq!(granted.output(), READ_MAPPING.to_le_bytes().to_vec());
    let denied = client.create("r.txt", GENERIC_WRITE, OPEN, 0).await;
    assert_eq!(denied.status(), status::ACCESS_DENIED);
    let maximum = open(&mut client, "r.txt", MAXIMUM_ALLOWED).await;
    let granted = query(&mut client, INFO_FILE, class::ACCESS, 0, maximum).await;
    let expected = access::allowed_mask(READER, false);
    assert_eq!(granted.output(), expected.to_le_bytes().to_vec());
    let system = client
        .create("r.txt", ACCESS_SYSTEM_SECURITY, OPEN, 0)
        .await;
    assert_eq!(system.status(), status::PRIVILEGE_NOT_HELD);
    let doomed = client
        .create("r.txt", FILE_READ_DATA, OPEN, DELETE_ON_CLOSE)
        .await;
    assert_eq!(
        doomed.status(),
        status::ACCESS_DENIED,
        "delete-on-close needs DELETE"
    );
    let fresh = client.create("new.txt", FILE_READ_DATA, CREATE, 0).await;
    assert_eq!(
        fresh.status(),
        status::ACCESS_DENIED,
        "creating needs write rights"
    );
    assert!(backend.data("new.txt").is_none());
}

#[tokio::test]
async fn read_needs_data() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("d.txt", ObjectKind::File, b"abcdef");
    let attributes = open(&mut client, "d.txt", FILE_READ_ATTRIBUTES).await;
    let refused = client
        .call(Command::Read, &read_body(attributes, 0, 3))
        .await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
    let reader = open(&mut client, "d.txt", FILE_READ_DATA).await;
    let data = client.call(Command::Read, &read_body(reader, 0, 3)).await;
    assert_eq!(data.status(), status::SUCCESS);
    assert_eq!(data.data(), b"abc");
    let past = client.call(Command::Read, &read_body(reader, 10, 3)).await;
    assert_eq!(past.status(), status::END_OF_FILE);
    let directory = open_directory(&mut client, "", FILE_READ_DATA).await;
    let refused = client
        .call(Command::Read, &read_body(directory, 0, 3))
        .await;
    assert_eq!(refused.status(), status::INVALID_DEVICE_REQUEST);
}

#[tokio::test]
async fn append_only_rules() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("log.txt", ObjectKind::File, b"1234");
    let appender = open(&mut client, "log.txt", FILE_APPEND_DATA).await;
    let interior = write(&mut client, appender, 0, b"x").await;
    assert_eq!(interior.status(), status::ACCESS_DENIED, "interior write");
    let crossing = write(&mut client, appender, 2, b"xyz").await;
    assert_eq!(
        crossing.status(),
        status::ACCESS_DENIED,
        "starts inside, crosses EOF"
    );
    let at_end = write(&mut client, appender, 4, b"56").await;
    assert_eq!(at_end.status(), status::SUCCESS);
    assert_eq!(written(&at_end), 2);
    let marker = write(&mut client, appender, END_OFFSET, b"78").await;
    assert_eq!(marker.status(), status::SUCCESS);
    assert_eq!(backend.data("log.txt"), Some(b"12345678".to_vec()));
    let empty = write(&mut client, appender, 0, b"").await;
    assert_eq!(empty.status(), status::SUCCESS);
    assert_eq!(written(&empty), 0);
    let overflow = write(&mut client, appender, u64::MAX - 1, b"ab").await;
    assert_eq!(overflow.status(), status::INVALID_PARAMETER);
    let writer = open(&mut client, "log.txt", FILE_WRITE_DATA).await;
    let crossing = write(&mut client, writer, 6, b"abcd").await;
    assert_eq!(crossing.status(), status::SUCCESS);
    let interior = write(&mut client, writer, 0, b"0").await;
    assert_eq!(interior.status(), status::SUCCESS);
    assert_eq!(backend.data("log.txt"), Some(b"023456abcd".to_vec()));
    let reader = open(&mut client, "log.txt", FILE_READ_DATA).await;
    let refused = write(&mut client, reader, 0, b"z").await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
}

#[tokio::test]
async fn write_charge_checked() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("big.bin", ObjectKind::File, b"");
    let writer = open(&mut client, "big.bin", FILE_WRITE_DATA).await;
    let payload = vec![7u8; 70_000];
    let short = client
        .call_charged(Command::Write, 1, &write_body(writer, 0, &payload))
        .await;
    assert_eq!(short.status(), status::INVALID_PARAMETER);
    let charged = client
        .call_charged(Command::Write, 2, &write_body(writer, 0, &payload))
        .await;
    assert_eq!(charged.status(), status::SUCCESS);
    assert_eq!(written(&charged), 70_000);
}

#[tokio::test]
async fn flush_needs_write() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("f.txt", ObjectKind::File, b"x");
    backend.put("dir", ObjectKind::Directory, b"");
    let reader = open(&mut client, "f.txt", FILE_READ_DATA).await;
    let refused = client.call(Command::Flush, &flush_body(reader)).await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
    for desired in [FILE_WRITE_DATA, FILE_APPEND_DATA] {
        let writer = open(&mut client, "f.txt", desired).await;
        let flushed = client.call(Command::Flush, &flush_body(writer)).await;
        assert_eq!(flushed.status(), status::SUCCESS);
    }
    let lister = open_directory(&mut client, "dir", FILE_LIST_DIRECTORY).await;
    let refused = client.call(Command::Flush, &flush_body(lister)).await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
    let adder = open_directory(&mut client, "dir", FILE_ADD_FILE).await;
    let flushed = client.call(Command::Flush, &flush_body(adder)).await;
    assert_eq!(flushed.status(), status::SUCCESS);
    let flushes = backend
        .state
        .lock()
        .calls
        .iter()
        .filter(|call| call.starts_with("flush"))
        .count();
    assert_eq!(flushes, 3);
}

#[tokio::test]
async fn listing_needs_list() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("dir", ObjectKind::Directory, b"");
    backend.put("dir/a.txt", ObjectKind::File, b"a");
    backend.put("dir/b.txt", ObjectKind::File, b"bb");
    backend.put("dir/c.md", ObjectKind::File, b"ccc");
    backend.put("dir/sub", ObjectKind::Directory, b"");
    backend.put("dir/sub/deep.txt", ObjectKind::File, b"d");
    let attributes = open_directory(&mut client, "dir", FILE_READ_ATTRIBUTES).await;
    let refused = client
        .call(
            Command::QueryDirectory,
            &directory_body(class::ID_BOTH_DIRECTORY, 0, attributes, "*", LIMIT),
        )
        .await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
    let lister = open_directory(&mut client, "dir", FILE_LIST_DIRECTORY).await;
    let listed = client
        .call(
            Command::QueryDirectory,
            &directory_body(class::ID_BOTH_DIRECTORY, 0, lister, "*", LIMIT),
        )
        .await;
    assert_eq!(listed.status(), status::SUCCESS);
    let found = entries(&listed.output());
    let found_names: Vec<&str> = found.iter().map(|(name, _, _)| name.as_str()).collect();
    assert_eq!(found_names, vec!["a.txt", "b.txt", "c.md", "sub"]);
    assert!(
        found
            .iter()
            .all(|(_, index, file_id)| *index == 0 && *file_id == 0),
        "V1 reports FileIndex and FileId zero"
    );
    let done = client
        .call(
            Command::QueryDirectory,
            &directory_body(class::ID_BOTH_DIRECTORY, 0, lister, "*", LIMIT),
        )
        .await;
    assert_eq!(done.status(), status::NO_MORE_FILES);
    let filtered = client
        .call(
            Command::QueryDirectory,
            &directory_body(
                class::ID_BOTH_DIRECTORY,
                messages::RESTART_SCANS,
                lister,
                "*.TXT",
                LIMIT,
            ),
        )
        .await;
    assert_eq!(names(&filtered.output()), vec!["a.txt", "b.txt"]);
    let nothing = client
        .call(
            Command::QueryDirectory,
            &directory_body(
                class::ID_BOTH_DIRECTORY,
                messages::RESTART_SCANS,
                lister,
                "zz*",
                LIMIT,
            ),
        )
        .await;
    assert_eq!(nothing.status(), status::NO_SUCH_FILE);
}

#[tokio::test]
async fn listing_pages_limits() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("dir", ObjectKind::Directory, b"");
    for name in ["a", "b", "c", "d"] {
        backend.put(&format!("dir/{name}"), ObjectKind::File, b"");
    }
    backend.state.lock().page_size = 1;
    let lister = open_directory(&mut client, "dir", FILE_LIST_DIRECTORY).await;
    let single = client
        .call(
            Command::QueryDirectory,
            &directory_body(
                class::ID_BOTH_DIRECTORY,
                messages::SINGLE_ENTRY,
                lister,
                "*",
                LIMIT,
            ),
        )
        .await;
    assert_eq!(names(&single.output()), vec!["a"]);
    let rest = client
        .call(
            Command::QueryDirectory,
            &directory_body(class::ID_BOTH_DIRECTORY, 0, lister, "*", LIMIT),
        )
        .await;
    assert_eq!(names(&rest.output()), vec!["b", "c", "d"]);
    let pages = backend
        .state
        .lock()
        .calls
        .iter()
        .filter(|call| *call == "list dir")
        .count();
    assert!(pages >= 4, "every backend slice is a separate list call");
    let tiny = client
        .call(
            Command::QueryDirectory,
            &directory_body(
                class::ID_BOTH_DIRECTORY,
                messages::RESTART_SCANS,
                lister,
                "*",
                50,
            ),
        )
        .await;
    assert_eq!(tiny.status(), status::INFO_LENGTH_MISMATCH);
    let unknown = client
        .call(
            Command::QueryDirectory,
            &directory_body(0x3C, messages::RESTART_SCANS, lister, "*", LIMIT),
        )
        .await;
    assert_eq!(unknown.status(), status::INVALID_INFO_CLASS);
}

#[tokio::test]
async fn query_classes_wire() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("q.txt", ObjectKind::File, b"hello");
    let reader = open(&mut client, "q.txt", FILE_READ_DATA).await;
    for file_class in [
        class::BASIC,
        class::ALL,
        class::NETWORK_OPEN,
        class::ATTRIBUTE_TAG,
    ] {
        let refused = query(&mut client, INFO_FILE, file_class, 0, reader).await;
        assert_eq!(
            refused.status(),
            status::ACCESS_DENIED,
            "class {file_class:#x}"
        );
    }
    let standard = query(&mut client, INFO_FILE, class::STANDARD, 0, reader).await;
    assert_eq!(standard.status(), status::SUCCESS);
    assert_eq!(standard.output()[8..16], 5u64.to_le_bytes());
    let internal = query(&mut client, INFO_FILE, class::INTERNAL, 0, reader).await;
    assert_eq!(internal.output(), vec![0u8; 8], "IndexNumber is zero");
    let extended = query(&mut client, INFO_FILE, class::EA, 0, reader).await;
    assert_eq!(extended.output(), vec![0u8; 4], "EaSize is zero");
    let full = query(&mut client, INFO_FILE, class::FULL_EA, 0, reader).await;
    assert_eq!(
        full.status(),
        status::ACCESS_DENIED,
        "FullEa needs FILE_READ_EA"
    );
    let normalized = query(&mut client, INFO_FILE, class::NORMALIZED_NAME, 0, reader).await;
    assert_eq!(normalized.status(), status::NOT_SUPPORTED);
    let unknown = query(&mut client, INFO_FILE, 0x77, 0, reader).await;
    assert_eq!(unknown.status(), status::INVALID_INFO_CLASS);
    let inspector = open(&mut client, "q.txt", FILE_READ_ATTRIBUTES | FILE_READ_EA).await;
    let basic = query(&mut client, INFO_FILE, class::BASIC, 0, inspector).await;
    assert_eq!(basic.output().len(), 40);
    let all = query(&mut client, INFO_FILE, class::ALL, 0, inspector).await;
    let output = all.output();
    assert_eq!(
        output[64..72],
        [0u8; 8],
        "IndexNumber inside FileAllInformation"
    );
    assert_eq!(output[100..], utf16_bytes("\\q.txt")[..]);
    let full = query(&mut client, INFO_FILE, class::FULL_EA, 0, inspector).await;
    assert_eq!(full.status(), status::NO_EXTENDED);
    let network = query(&mut client, INFO_FILE, class::NETWORK_OPEN, 0, inspector).await;
    assert_eq!(network.output().len(), 56);
    let tag = query(&mut client, INFO_FILE, class::ATTRIBUTE_TAG, 0, inspector).await;
    assert_eq!(tag.output().len(), 8);
    let short = query_sized(&mut client, INFO_FILE, class::BASIC, 0, inspector, 8).await;
    assert_eq!(short.status(), status::INFO_LENGTH_MISMATCH);
    let partial = query_sized(&mut client, INFO_FILE, class::ALL, 0, inspector, 70).await;
    assert_eq!(partial.status(), status::BUFFER_OVERFLOW);
    assert_eq!(partial.output().len(), 70);
    let size = query(
        &mut client,
        INFO_FILESYSTEM,
        volume::FULL_SIZE,
        0,
        inspector,
    )
    .await;
    assert_eq!(size.output().len(), 32);
    let kind = query(
        &mut client,
        INFO_FILESYSTEM,
        volume::ATTRIBUTE,
        0,
        inspector,
    )
    .await;
    assert!(kind.output().ends_with(&utf16_bytes("NTFS")));
}

#[tokio::test]
async fn security_queries_wire() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("s.txt", ObjectKind::File, b"x");
    let controller = open(&mut client, "s.txt", READ_CONTROL | FILE_READ_DATA).await;
    let owner = query(&mut client, INFO_SECURITY, 0, parts::OWNER, controller).await;
    assert_eq!(owner.status(), status::SUCCESS);
    assert_eq!(owner.output().len(), 32);
    for additional in [parts::SACL, parts::BACKUP, parts::DACL | parts::SACL] {
        let refused = query(&mut client, INFO_SECURITY, 0, additional, controller).await;
        assert_eq!(
            refused.status(),
            status::ACCESS_DENIED,
            "mask {additional:#x}"
        );
    }
    let small = query_sized(&mut client, INFO_SECURITY, 0, parts::DACL, controller, 8).await;
    assert_eq!(small.status(), status::BUFFER_TOO_SMALL);
    assert_eq!(small.body()[8..12], 48u32.to_le_bytes(), "the needed size");
    let reader = open(&mut client, "s.txt", FILE_READ_DATA).await;
    let refused = query(&mut client, INFO_SECURITY, 0, parts::DACL, reader).await;
    assert_eq!(refused.status(), status::ACCESS_DENIED);
}

#[tokio::test]
async fn set_classes_wire() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("s.txt", ObjectKind::File, b"abcdef");
    let reader = open(&mut client, "s.txt", FILE_READ_DATA).await;
    let mut basic = vec![0u8; 32];
    basic.extend_from_slice(&0x20u32.to_le_bytes());
    basic.extend_from_slice(&[0u8; 4]);
    let cases: [(u8, Vec<u8>); 6] = [
        (class::BASIC, basic.clone()),
        (class::END_OF_FILE, 3u64.to_le_bytes().to_vec()),
        (class::ALLOCATION, 4096u64.to_le_bytes().to_vec()),
        (class::DISPOSITION, vec![1]),
        (class::DISPOSITION_EX, 1u32.to_le_bytes().to_vec()),
        (class::RENAME, rename_buffer(false, "s.txt")),
    ];
    for (file_class, buffer) in &cases {
        let refused = set(&mut client, *file_class, reader, buffer).await;
        assert_eq!(
            refused.status(),
            status::ACCESS_DENIED,
            "class {file_class:#x}"
        );
    }
    let attributes = open(&mut client, "s.txt", FILE_WRITE_ATTRIBUTES).await;
    assert_eq!(
        set(&mut client, class::BASIC, attributes, &basic)
            .await
            .status(),
        status::SUCCESS
    );
    let writer = open(&mut client, "s.txt", FILE_WRITE_DATA).await;
    let truncate = set(&mut client, class::END_OF_FILE, writer, &3u64.to_le_bytes()).await;
    assert_eq!(truncate.status(), status::SUCCESS);
    assert_eq!(backend.data("s.txt"), Some(b"abc".to_vec()));
    let reserve = set(
        &mut client,
        class::ALLOCATION,
        writer,
        &4096u64.to_le_bytes(),
    )
    .await;
    assert_eq!(reserve.status(), status::SUCCESS);
    let deleter = open(&mut client, "s.txt", DELETE).await;
    let same = set(
        &mut client,
        class::RENAME,
        deleter,
        &rename_buffer(false, "s.txt"),
    )
    .await;
    assert_eq!(
        same.status(),
        status::SUCCESS,
        "a same-name rename with DELETE"
    );
    let link = set(
        &mut client,
        class::LINK,
        deleter,
        &rename_buffer(false, "t.txt"),
    )
    .await;
    assert_eq!(link.status(), status::NOT_SUPPORTED);
    let pending = set(&mut client, class::DISPOSITION, deleter, &[1]).await;
    assert_eq!(pending.status(), status::SUCCESS);
    for file_id in [reader, attributes, writer, deleter] {
        assert_eq!(client.close(file_id).await.status(), status::SUCCESS);
    }
    assert!(
        backend.data("s.txt").is_none(),
        "the last close deletes the file"
    );
}

#[tokio::test]
async fn rename_rules_wire() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("dir", ObjectKind::Directory, b"");
    backend.put("x.txt", ObjectKind::File, b"x");
    backend.put("y.txt", ObjectKind::File, b"y");
    let mover = open(&mut client, "x.txt", DELETE | FILE_READ_ATTRIBUTES).await;
    let full = rename_buffer(false, "z");
    let short = set(&mut client, class::RENAME, mover, &full[..23]).await;
    assert_eq!(short.status(), status::INFO_LENGTH_MISMATCH);
    let mut rooted = rename_buffer(false, "z.txt");
    rooted[8] = 1;
    assert_eq!(
        set(&mut client, class::RENAME, mover, &rooted)
            .await
            .status(),
        status::INVALID_PARAMETER
    );
    let mut odd = rename_buffer(false, "z.txt");
    odd[16] = 3;
    assert_eq!(
        set(&mut client, class::RENAME, mover, &odd).await.status(),
        status::INVALID_PARAMETER
    );
    for name in ["..\\x.txt", "\\abs.txt", "dir\\..\\..\\x", "a:b"] {
        let refused = set(
            &mut client,
            class::RENAME,
            mover,
            &rename_buffer(false, name),
        )
        .await;
        assert_eq!(refused.status(), status::NAME_INVALID, "{name}");
    }
    let taken = set(
        &mut client,
        class::RENAME,
        mover,
        &rename_buffer(false, "y.txt"),
    )
    .await;
    assert_eq!(taken.status(), status::NAME_COLLISION);
    let moved = set(
        &mut client,
        class::RENAME,
        mover,
        &rename_buffer(false, "dir\\moved.txt"),
    )
    .await;
    assert_eq!(moved.status(), status::SUCCESS);
    assert_eq!(backend.data("dir/moved.txt"), Some(b"x".to_vec()));
    assert!(backend.data("x.txt").is_none());
    let all = query(&mut client, INFO_FILE, class::ALL, 0, mover).await;
    assert_eq!(
        all.status(),
        status::SUCCESS,
        "the FileId survives the rename"
    );
    assert_eq!(all.output()[100..], utf16_bytes("\\dir\\moved.txt")[..]);
}

#[tokio::test]
async fn file_scope_enforced() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"abc");
    let file_id = open(&mut client, "a.txt", FILE_READ_DATA).await;
    let first_tree = client.tree_id;
    let second = client.connect(crate::testing::BUCKET).await;
    assert_eq!(second.status(), status::SUCCESS);
    client.tree_id = second.header.tree_id;
    assert_ne!(client.tree_id, first_tree);
    let other_tree = client.call(Command::Read, &read_body(file_id, 0, 1)).await;
    assert_eq!(other_tree.status(), status::FILE_CLOSED);
    client.tree_id = first_tree;
    let forged = FileId {
        persistent: file_id.persistent ^ 1,
        volatile: file_id.volatile,
    };
    let forged = client.call(Command::Read, &read_body(forged, 0, 1)).await;
    assert_eq!(forged.status(), status::FILE_CLOSED);
    let genuine = client.call(Command::Read, &read_body(file_id, 0, 1)).await;
    assert_eq!(genuine.status(), status::SUCCESS);
}

#[tokio::test]
async fn create_name_rules() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("dir", ObjectKind::Directory, b"");
    backend.put("x.txt", ObjectKind::File, b"x");
    let leading = client.create("\\lead.txt", FILE_READ_DATA, OPEN, 0).await;
    assert_eq!(leading.status(), status::INVALID_PARAMETER);
    for name in ["a:b", "CON", "dir\\..\\x.txt", "trail.", "dir\\\\x"] {
        let refused = client.create(name, FILE_READ_DATA, OPEN, 0).await;
        assert_eq!(refused.status(), status::NAME_INVALID, "{name}");
    }
    let file_as_directory = client
        .create("x.txt", FILE_READ_DATA, OPEN, DIRECTORY_FILE)
        .await;
    assert_eq!(file_as_directory.status(), status::NOT_DIRECTORY);
    let directory_as_file = client
        .create("dir", FILE_READ_DATA, OPEN, NON_DIRECTORY)
        .await;
    assert_eq!(directory_as_file.status(), status::IS_DIRECTORY);
    let collision = client.create("x.txt", FILE_READ_DATA, CREATE, 0).await;
    assert_eq!(collision.status(), status::NAME_COLLISION);
    let orphan = client
        .create("nope\\x.txt", FILE_READ_DATA, CREATE, 0)
        .await;
    assert_eq!(orphan.status(), status::PATH_NOT_FOUND);
    let created = client
        .create("dir\\new.txt", FILE_WRITE_DATA, CREATE, 0)
        .await;
    assert_eq!(created.status(), status::SUCCESS);
    assert_eq!(backend.data("dir/new.txt"), Some(Vec::new()));
}
