//! Wire tests of the connection engine: strict negotiation, NTLMv2 logons, encryption,
//! compounds, interim responses, cancellation, revocation and the server handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::access::{FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_WRITE_DATA, GENERIC_WRITE};
use crate::backend::ObjectKind;
use crate::crypto::{
    CIPHER_AES128_CCM, CIPHER_AES256_CCM, CIPHER_AES256_GCM, Cipher, HASH_SHA512, SIGNING_HMAC,
    Signing,
};
use crate::messages::{LOCK_EXCLUSIVE, LOCK_IMMEDIATE, LOCK_UNLOCK};
use crate::spnego::{ClientToken, NTLMSSP_OID, parse_client};
use crate::testing::{
    ACCESS_KEY, BUCKET, MockBackend, Response, SECRET, TestClient, Wrapping, close_body,
    create_body, default_contexts, list_context, lock_body, logged_in, negotiate_request,
    read_body, start, test_limits, tight_limits,
};
use crate::wire::{FLAG_SIGNED, FileId, RELATED_FILE};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

const OPEN: u32 = 1;

async fn assert_closed(client: &mut TestClient) {
    assert!(
        client.receive_raw().await.is_none(),
        "the server closed the connection"
    );
}

fn security_of(response: &Response) -> Vec<u8> {
    let body = response.body();
    let offset = u16::from_le_bytes([body[56], body[57]]) as usize;
    let length = u16::from_le_bytes([body[58], body[59]]) as usize;
    response.raw[offset..offset + length].to_vec()
}

#[tokio::test]
async fn negotiate_selects_gcm() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let response = client.negotiate().await;
    assert_eq!(response.status(), status::SUCCESS);
    let body = response.body();
    assert_eq!(u16::from_le_bytes([body[4], body[5]]), 0x0311);
    assert_eq!(
        u16::from_le_bytes([body[2], body[3]]),
        0x0003,
        "signing required"
    );
    let capabilities = u32::from_le_bytes([body[24], body[25], body[26], body[27]]);
    assert_eq!(
        capabilities & 0x40,
        0,
        "3.1.1 negotiates encryption by context"
    );
    assert_eq!(
        client.cipher,
        Cipher::Aes256Gcm,
        "the client's first GCM cipher"
    );
    assert_eq!(client.signing, Signing::AesGmac);
    let ClientToken::Init(hint) = parse_client(&security_of(&response)).expect("hint") else {
        panic!("the hint is a NegTokenInit");
    };
    assert_eq!(hint.mechanisms, vec![NTLMSSP_OID.to_vec()]);
}

#[tokio::test]
async fn refuses_old_dialects() {
    let (mut client, harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let response = client
        .negotiate_with(&[0x0202, 0x0210, 0x0300, 0x0302], &default_contexts())
        .await
        .expect("an answer before closing");
    assert_eq!(response.status(), status::NOT_SUPPORTED);
    assert_closed(&mut client).await;
    harness.task.await.expect("connection task ends");
}

#[tokio::test]
async fn refuses_legacy_probe() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let mut probe = b"\xFFSMBr".to_vec();
    probe.extend_from_slice(&[0u8; 40]);
    client.send_raw(&probe).await;
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn refuses_ccm_ciphers() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let contexts = vec![
        list_context(0x0001, &[HASH_SHA512]),
        list_context(0x0002, &[CIPHER_AES128_CCM, CIPHER_AES256_CCM]),
    ];
    let response = client
        .negotiate_with(&[0x0311], &contexts)
        .await
        .expect("answer");
    assert_eq!(response.status(), status::NOT_SUPPORTED);
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn signing_offer_honoured() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let absent = vec![
        list_context(0x0001, &[HASH_SHA512]),
        list_context(0x0002, &[CIPHER_AES256_GCM]),
    ];
    let response = client
        .negotiate_with(&[0x0311], &absent)
        .await
        .expect("answer");
    assert_eq!(response.status(), status::SUCCESS);
    assert_eq!(client.signing, Signing::AesCmac, "the dialect default");
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let mut offered = absent.clone();
    offered.push(list_context(0x0008, &[SIGNING_HMAC]));
    let response = client
        .negotiate_with(&[0x0311], &offered)
        .await
        .expect("answer");
    assert_eq!(response.status(), status::NOT_SUPPORTED);
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn requires_preauth_context() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let contexts = vec![list_context(0x0002, &[crypto::CIPHER_AES128_GCM])];
    let response = client
        .negotiate_with(&[0x0311], &contexts)
        .await
        .expect("answer");
    assert_eq!(response.status(), status::INVALID_PARAMETER);
    assert_closed(&mut client).await;
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    let contexts = vec![
        list_context(0x0001, &[0x0002]),
        list_context(0x0002, &[crypto::CIPHER_AES128_GCM]),
    ];
    let response = client
        .negotiate_with(&[0x0311], &contexts)
        .await
        .expect("answer");
    assert_eq!(
        response.status(),
        status::NOT_SUPPORTED,
        "SHA-512 is required"
    );
}

#[tokio::test]
async fn login_derives_keys() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Spnego).await;
    assert_eq!(response.status(), status::SUCCESS);
    assert_ne!(response.header.flags & FLAG_SIGNED, 0);
    let session_flags = u16::from_le_bytes([response.body()[2], response.body()[3]]);
    assert_eq!(
        session_flags, 0x0004,
        "ENCRYPT_DATA and never guest or null"
    );
    assert!(
        client.server_mic.is_some(),
        "the server answers with a mechListMIC"
    );
    let tree = client.connect(BUCKET).await;
    assert_eq!(tree.status(), status::SUCCESS);
}

#[tokio::test]
async fn mic_policy_applies() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Bare).await;
    assert_eq!(response.status(), status::LOGON_FAILURE);
    let config = SmbConfig {
        mic_policy: MicPolicy::AllowMissing,
        ..SmbConfig::default()
    };
    let (mut client, _harness) = start(MockBackend::new(), config, test_limits());
    client.negotiate().await;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Bare).await;
    assert_eq!(response.status(), status::SUCCESS);
    assert!(client.server_mic.is_none());
}

#[tokio::test]
async fn wrong_secret_fails() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client
        .setup(ACCESS_KEY, "not the secret", Wrapping::Spnego)
        .await;
    assert_eq!(response.status(), status::LOGON_FAILURE);
    client.session_id = 0;
    let response = client.setup("akia-tester", SECRET, Wrapping::Spnego).await;
    assert_eq!(
        response.status(),
        status::LOGON_FAILURE,
        "no case-folded alias"
    );
    client.session_id = 0;
    let response = client.setup("nobody", SECRET, Wrapping::Spnego).await;
    assert_eq!(response.status(), status::LOGON_FAILURE);
    client.session_id = 0;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Spnego).await;
    assert_eq!(
        response.status(),
        status::SUCCESS,
        "the connection survives failures"
    );
}

#[tokio::test]
async fn raw_ntlmssp_login() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Raw).await;
    assert_eq!(response.status(), status::SUCCESS);
    assert_eq!(client.connect(BUCKET).await.status(), status::SUCCESS);
}

#[tokio::test]
async fn admission_failure_refuses() {
    let backend = MockBackend::new();
    backend.state.lock().revoked.insert(ACCESS_KEY.to_owned());
    let (mut client, _harness) = start(backend, SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client.setup(ACCESS_KEY, SECRET, Wrapping::Spnego).await;
    assert_eq!(response.status(), status::LOGON_FAILURE);
}

#[tokio::test]
async fn plaintext_after_login() {
    let (mut client, _harness, _backend) = logged_in().await;
    let response = client
        .plain(Command::Echo, &crate::messages::empty_body())
        .await;
    assert_eq!(response.status(), status::ACCESS_DENIED);
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn echo_before_login() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), test_limits());
    client.negotiate().await;
    let response = client
        .plain(Command::Echo, &crate::messages::empty_body())
        .await;
    assert_eq!(response.status(), status::SUCCESS);
    let response = client
        .plain(Command::TreeConnect, &[9, 0, 0, 0, 0, 0, 0, 0, 0])
        .await;
    assert_eq!(response.status(), status::USER_SESSION_DELETED);
}

#[tokio::test]
async fn tampered_transform_closes() {
    let (mut client, _harness, _backend) = logged_in().await;
    let header = client.header(Command::Echo, 1);
    let message = TestClient::message(&header, &crate::messages::empty_body());
    let mut packet = client.seal(&message);
    let last = packet.len() - 1;
    packet[last] ^= 0x01;
    client.send_raw(&packet).await;
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn reused_message_closes() {
    let (mut client, _harness, _backend) = logged_in().await;
    let mut header = client.header(Command::Echo, 1);
    let response = client
        .call(Command::Echo, &crate::messages::empty_body())
        .await;
    assert_eq!(response.status(), status::SUCCESS);
    header.message_id = response.header.message_id;
    let message = TestClient::message(&header, &crate::messages::empty_body());
    client.send_sealed(&message).await;
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn handshake_rate_limited() {
    let (mut client, _harness) = start(MockBackend::new(), SmbConfig::default(), tight_limits(1));
    assert_eq!(client.negotiate().await.status(), status::SUCCESS);
    let header = client.header(Command::SessionSetup, 1);
    let mut body = vec![25, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 88, 0, 0, 0];
    body.extend_from_slice(&[0u8; 9]);
    let message = TestClient::message(&header, &body);
    client.send_raw(&message).await;
    assert_closed(&mut client).await;
}

fn related(client: &mut TestClient, command: Command, body: &[u8]) -> Vec<u8> {
    let mut header = client.header(command, 1);
    header.flags |= wire::FLAG_RELATED;
    header.session_id = u64::MAX;
    header.tree_id = u32::MAX;
    TestClient::message(&header, body)
}

#[tokio::test]
async fn compound_propagates_ids() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"hello");
    let create = {
        let header = client.header(Command::Create, 1);
        TestClient::message(
            &header,
            &create_body("a.txt", FILE_READ_DATA | FILE_READ_ATTRIBUTES, OPEN, 0),
        )
    };
    let read = related(&mut client, Command::Read, &read_body(RELATED_FILE, 0, 5));
    let close = related(&mut client, Command::Close, &close_body(RELATED_FILE, 0));
    client
        .send_sealed(&wire::join_compound(vec![create, read, close]))
        .await;
    let response = client.receive().await.expect("compound response");
    let parts = wire::split_compound(&response.raw).expect("compound splits");
    assert_eq!(parts.len(), 3);
    let replies: Vec<Response> = parts
        .iter()
        .map(|part| Response {
            header: Header::decode(part).expect("header"),
            raw: part.to_vec(),
        })
        .collect();
    for reply in &replies {
        assert_eq!(reply.status(), status::SUCCESS);
        assert_eq!(reply.header.session_id, client.session_id);
        assert_eq!(reply.header.tree_id, client.tree_id);
    }
    assert_ne!(replies[1].header.flags & wire::FLAG_RELATED, 0);
    assert_eq!(replies[1].data(), b"hello");
    assert!(
        backend.state.lock().handles.is_empty(),
        "the related CLOSE closed the open"
    );
}

#[tokio::test]
async fn compound_failure_propagates() {
    let (mut client, _harness, _backend) = logged_in().await;
    let create = {
        let header = client.header(Command::Create, 1);
        TestClient::message(
            &header,
            &create_body("missing.txt", FILE_READ_DATA, OPEN, 0),
        )
    };
    let read = related(&mut client, Command::Read, &read_body(RELATED_FILE, 0, 5));
    client
        .send_sealed(&wire::join_compound(vec![create, read]))
        .await;
    let response = client.receive().await.expect("compound response");
    let parts = wire::split_compound(&response.raw).expect("compound splits");
    let statuses: Vec<u32> = parts
        .iter()
        .map(|part| Header::decode(part).expect("header").status)
        .collect();
    assert_eq!(
        statuses,
        vec![status::NAME_NOT_FOUND, status::NAME_NOT_FOUND]
    );
}

async fn two_opens(client: &mut TestClient, backend: &MockBackend) -> (FileId, FileId) {
    backend.put("lock.bin", ObjectKind::File, &[0u8; 16]);
    let first = client
        .create("lock.bin", FILE_READ_DATA | FILE_WRITE_DATA, OPEN, 0)
        .await;
    let second = client
        .create("lock.bin", FILE_READ_DATA | FILE_WRITE_DATA, OPEN, 0)
        .await;
    assert_eq!(first.status(), status::SUCCESS);
    assert_eq!(second.status(), status::SUCCESS);
    (first.file_id(), second.file_id())
}

#[tokio::test(start_paused = true)]
async fn pending_lock_completes() {
    let (mut client, _harness, backend) = logged_in().await;
    let (holder, waiter) = two_opens(&mut client, &backend).await;
    let taken = client
        .call(
            Command::Lock,
            &lock_body(holder, &[(0, 8, LOCK_EXCLUSIVE | LOCK_IMMEDIATE)]),
        )
        .await;
    assert_eq!(taken.status(), status::SUCCESS);
    let refused = client
        .call(
            Command::Lock,
            &lock_body(waiter, &[(0, 8, LOCK_EXCLUSIVE | LOCK_IMMEDIATE)]),
        )
        .await;
    assert_eq!(refused.status(), status::LOCK_NOT_GRANTED);
    let interim = client
        .call(Command::Lock, &lock_body(waiter, &[(0, 8, LOCK_EXCLUSIVE)]))
        .await;
    assert_eq!(interim.status(), status::PENDING);
    assert_ne!(interim.header.flags & FLAG_ASYNC, 0);
    assert_ne!(interim.header.async_id, 0);
    assert!(
        interim.header.credits > 0,
        "the interim response grants credits"
    );
    let header = client.header(Command::Lock, 1);
    let unlock = TestClient::message(&header, &lock_body(holder, &[(0, 8, LOCK_UNLOCK)]));
    client.send_sealed(&unlock).await;
    let mut answers = [
        client.receive().await.expect("answer"),
        client.receive().await.expect("answer"),
    ];
    answers.sort_by_key(|answer| answer.header.message_id);
    let (pending, unlocked) = (&answers[0], &answers[1]);
    assert_eq!(unlocked.header.message_id, header.message_id);
    assert_eq!(unlocked.status(), status::SUCCESS);
    assert_eq!(pending.header.message_id, interim.header.message_id);
    assert_eq!(pending.header.async_id, interim.header.async_id);
    assert_ne!(pending.header.flags & FLAG_ASYNC, 0);
    assert_eq!(
        pending.status(),
        status::SUCCESS,
        "a final response follows the interim"
    );
    assert_eq!(pending.header.credits, 0);
}

#[tokio::test(start_paused = true)]
async fn cancel_ends_pending() {
    let (mut client, _harness, backend) = logged_in().await;
    let (holder, waiter) = two_opens(&mut client, &backend).await;
    let taken = client
        .call(
            Command::Lock,
            &lock_body(holder, &[(0, 8, LOCK_EXCLUSIVE | LOCK_IMMEDIATE)]),
        )
        .await;
    assert_eq!(taken.status(), status::SUCCESS);
    let interim = client
        .call(Command::Lock, &lock_body(waiter, &[(0, 8, LOCK_EXCLUSIVE)]))
        .await;
    assert_eq!(interim.status(), status::PENDING);
    let cancel = Header {
        command: Command::Cancel.code(),
        flags: FLAG_ASYNC,
        message_id: interim.header.message_id,
        async_id: interim.header.async_id,
        session_id: client.session_id,
        ..Header::default()
    };
    client
        .send_sealed(&TestClient::message(
            &cancel,
            &crate::messages::empty_body(),
        ))
        .await;
    let last = client.receive().await.expect("final response");
    assert_eq!(last.header.message_id, interim.header.message_id);
    assert_eq!(last.header.async_id, interim.header.async_id);
    assert_eq!(last.status(), status::CANCELLED);
    assert_eq!(
        backend.state.lock().cancelled.len(),
        1,
        "the backend hears of it"
    );
    let still = client
        .call(Command::Echo, &crate::messages::empty_body())
        .await;
    assert_eq!(still.status(), status::SUCCESS, "CANCEL consumed no credit");
}

#[tokio::test(start_paused = true)]
async fn plaintext_cancel_ignored() {
    let (mut client, _harness, backend) = logged_in().await;
    let (holder, waiter) = two_opens(&mut client, &backend).await;
    let taken = client
        .call(
            Command::Lock,
            &lock_body(holder, &[(0, 8, LOCK_EXCLUSIVE | LOCK_IMMEDIATE)]),
        )
        .await;
    assert_eq!(taken.status(), status::SUCCESS);
    let interim = client
        .call(Command::Lock, &lock_body(waiter, &[(0, 8, LOCK_EXCLUSIVE)]))
        .await;
    assert_eq!(interim.status(), status::PENDING);
    let cancel = Header {
        command: Command::Cancel.code(),
        flags: FLAG_ASYNC,
        message_id: interim.header.message_id,
        async_id: interim.header.async_id,
        session_id: client.session_id,
        ..Header::default()
    };
    let message = TestClient::message(&cancel, &crate::messages::empty_body());
    client.send_raw(&message).await;
    let still = client
        .call(Command::Echo, &crate::messages::empty_body())
        .await;
    assert_eq!(still.status(), status::SUCCESS);
    assert!(backend.state.lock().cancelled.is_empty(), "still waiting");
    client.send_sealed(&message).await;
    let last = client.receive().await.expect("final response");
    assert_eq!(last.header.message_id, interim.header.message_id);
    assert_eq!(last.status(), status::CANCELLED);
}

#[tokio::test(start_paused = true)]
async fn disconnect_ends_lock() {
    let (mut client, harness, backend) = logged_in().await;
    let (holder, waiter) = two_opens(&mut client, &backend).await;
    let taken = client
        .call(
            Command::Lock,
            &lock_body(holder, &[(0, 8, LOCK_EXCLUSIVE | LOCK_IMMEDIATE)]),
        )
        .await;
    assert_eq!(taken.status(), status::SUCCESS);
    let interim = client
        .call(Command::Lock, &lock_body(waiter, &[(0, 8, LOCK_EXCLUSIVE)]))
        .await;
    assert_eq!(interim.status(), status::PENDING);
    drop(client);
    let ended = tokio::time::timeout(Duration::from_secs(60), harness.task).await;
    assert!(ended.is_ok(), "a waiting LOCK does not hold the connection");
    assert!(
        backend.state.lock().handles.is_empty(),
        "the disconnect closes every open"
    );
}

#[tokio::test]
async fn revocation_expires_session() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"x");
    let open = client.create("a.txt", FILE_READ_DATA, OPEN, 0).await;
    assert_eq!(open.status(), status::SUCCESS);
    backend.revoke(ACCESS_KEY);
    let response = client.create("a.txt", FILE_READ_DATA, OPEN, 0).await;
    assert_eq!(response.status(), status::NETWORK_SESSION_EXPIRED);
    let response = client
        .call(Command::Read, &read_body(open.file_id(), 0, 1))
        .await;
    assert_eq!(response.status(), status::NETWORK_SESSION_EXPIRED);
    let state = backend.state.lock();
    assert!(
        state.handles.is_empty(),
        "opens of the expired session are closed"
    );
}

#[tokio::test]
async fn job_view_routes() {
    let backend = MockBackend::new();
    let job = Ulid::from(42u128);
    backend
        .state
        .lock()
        .job_views
        .insert(job, ACCESS_KEY.to_owned());
    let (mut client, _harness) = start(backend, SmbConfig::default(), test_limits());
    client.login().await;
    let view = client.connect(&format!("in-{job}")).await;
    assert_eq!(view.status(), status::SUCCESS);
    let share_flags = u32::from_le_bytes(view.body()[4..8].try_into().expect("flags"));
    assert_eq!(share_flags & 0x30, 0x30, "client-side caching is off");
    assert_ne!(share_flags & 0x8000, 0);
    let maximal = u32::from_le_bytes(view.body()[12..16].try_into().expect("access"));
    assert_eq!(maximal & FILE_WRITE_DATA, 0, "a job view is read-only");
    client.tree_id = view.header.tree_id;
    let response = client.create("new.txt", GENERIC_WRITE, 2, 0).await;
    assert_eq!(response.status(), status::ACCESS_DENIED);
    let other = Ulid::from(43u128);
    assert_eq!(
        client.connect(&format!("in-{other}")).await.status(),
        status::ACCESS_DENIED
    );
    for name in ["in-notajob", "IPC$", "Data", "C$"] {
        assert_eq!(
            client.connect(name).await.status(),
            status::BAD_NETWORK_NAME,
            "{name}"
        );
    }
}

#[tokio::test]
async fn logoff_ends_session() {
    let (mut client, _harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"x");
    assert_eq!(
        client
            .create("a.txt", FILE_READ_DATA, OPEN, 0)
            .await
            .status(),
        status::SUCCESS
    );
    let response = client
        .call(Command::Logoff, &crate::messages::empty_body())
        .await;
    assert_eq!(response.status(), status::SUCCESS);
    assert!(backend.state.lock().handles.is_empty());
    let header = client.header(Command::Echo, 1);
    client
        .send_sealed(&TestClient::message(
            &header,
            &crate::messages::empty_body(),
        ))
        .await;
    assert_closed(&mut client).await;
}

async fn tcp_negotiate(address: SocketAddr) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect");
    let header = Header {
        command: Command::Negotiate.code(),
        credits: 1,
        ..Header::default()
    };
    let message = TestClient::message(&header, &negotiate_request(&[0x0311], &default_contexts()));
    stream
        .write_all(&wire::frame(&message).expect("frame"))
        .await
        .expect("send");
    let reply = wire::read_frame(&mut stream, wire::TRANSPORT_LIMIT)
        .await
        .expect("frame")
        .expect("response");
    assert_eq!(
        Header::decode(&reply).expect("header").status,
        status::SUCCESS
    );
    stream
}

#[tokio::test]
async fn handle_drains_gracefully() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server =
        SmbServer::new(MockBackend::new(), test_limits(), SmbConfig::default()).expect("server");
    let shutdown = CancellationToken::new();
    let (address, mut handle) = server
        .run_with_listener(listener, shutdown.clone())
        .expect("run");
    let mut stream = tcp_negotiate(address).await;
    assert!(!handle.is_finished());
    shutdown.cancel();
    handle.wait_until_released().await;
    assert!(handle.is_finished());
    assert_eq!(handle.connection_count(), 0);
    let closed = wire::read_frame(&mut stream, wire::TRANSPORT_LIMIT).await;
    assert!(
        matches!(closed, Ok(None) | Err(_)),
        "the server closed the socket"
    );
}

#[tokio::test]
async fn handle_aborts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let server =
        SmbServer::new(MockBackend::new(), test_limits(), SmbConfig::default()).expect("server");
    let (address, mut handle) = server
        .run_with_listener(listener, CancellationToken::new())
        .expect("run");
    let _stream = tcp_negotiate(address).await;
    handle.abort();
    assert_eq!(handle.exit("SMB").await, "SMB server aborted");
    handle.wait_until_released().await;
    assert!(handle.is_finished());
}

#[tokio::test]
async fn shutdown_closes_opens() {
    let (mut client, harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"x");
    let open = client.create("a.txt", FILE_READ_DATA, OPEN, 0).await;
    assert_eq!(open.status(), status::SUCCESS);
    harness.shutdown.cancel();
    harness.task.await.expect("connection task ends");
    assert!(
        backend.state.lock().handles.is_empty(),
        "graceful shutdown closes opens"
    );
    assert_closed(&mut client).await;
}

#[tokio::test]
async fn abort_stops_connection() {
    let (mut client, harness, backend) = logged_in().await;
    backend.put("a.txt", ObjectKind::File, b"x");
    let open = client.create("a.txt", FILE_READ_DATA, OPEN, 0).await;
    assert_eq!(open.status(), status::SUCCESS);
    harness.abort.cancel();
    harness.task.await.expect("connection task ends");
    assert_eq!(backend.state.lock().handles.len(), 1, "abort skips cleanup");
    assert_closed(&mut client).await;
}
