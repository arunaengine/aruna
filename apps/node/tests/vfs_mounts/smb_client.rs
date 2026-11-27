//! An SMB 3.1.1 client over loopback TCP: SHA-512 preauthentication, AES-128-GCM sealing and
//! an NTLMv2 initiator built on the public wire and crypto modules of `aruna-smb`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;

use aruna_smb::crypto::{
    self, CIPHER_AES128_GCM, Cipher, HASH_SHA512, PreauthHash, SIGNING_CMAC, Sealer, SessionKeys,
    Signing,
};
use aruna_smb::ntlm;
use aruna_smb::status;
use aruna_smb::wire::{self, Command, FileId, HEADER_SIZE, Header, utf16_bytes};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::shared::{S3Credentials, TestResult};

pub(crate) const READ_DATA: u32 = 0x0000_0001;
pub(crate) const WRITE_DATA: u32 = 0x0000_0002;
pub(crate) const READ_ATTRIBUTES: u32 = 0x0000_0080;
pub(crate) const DELETE: u32 = 0x0001_0000;
/// Read, write and delete access of a writer open.
pub(crate) const WRITER: u32 = READ_DATA | WRITE_DATA | READ_ATTRIBUTES | DELETE;
pub(crate) const FILE_OPEN: u32 = 1;
pub(crate) const FILE_CREATE: u32 = 2;
pub(crate) const DIRECTORY: u32 = 0x0000_0001;
pub(crate) const NOT_DIRECTORY: u32 = 0x0000_0040;
/// The largest READ or WRITE payload one credit pays for.
pub(crate) const IO_CHUNK: usize = 64 * 1024;
const DIALECT_311: u16 = 0x0311;
const RENAME_CLASS: u8 = 10;
const DISPOSITION_CLASS: u8 = 13;
const FILE_INFO: u8 = 1;

/// Little-endian request body under construction.
#[derive(Default)]
struct Body(Vec<u8>);

impl Body {
    fn u8(mut self, value: u8) -> Self {
        self.0.push(value);
        self
    }

    fn u16(mut self, value: u16) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    fn u32(mut self, value: u32) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    fn u64(mut self, value: u64) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    fn file(mut self, file: FileId) -> Self {
        file.write(&mut self.0);
        self
    }

    /// Appends `data`, or the one padding byte SMB2 asks for in place of an empty buffer.
    fn tail(mut self, data: &[u8]) -> Self {
        if data.is_empty() {
            self.0.push(0);
        } else {
            self.0.extend_from_slice(data);
        }
        self
    }
}

/// A LOCK request body for one range of `file`.
fn lock_body(file: FileId, (at, length): (u64, u64), flags: u32) -> Vec<u8> {
    let body = Body::default().u16(48).u16(1).u32(0).file(file);
    body.u64(at).u64(length).u32(flags).u32(0).0
}

/// A buffer offset counted from the start of the SMB2 header.
fn offset(body_bytes: usize) -> u16 {
    (HEADER_SIZE + body_bytes) as u16
}

fn word(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn half(bytes: &[u8], at: usize) -> usize {
    usize::from(u16::from_le_bytes([bytes[at], bytes[at + 1]]))
}

/// One decrypted response.
pub(crate) struct Reply {
    pub(crate) header: Header,
    raw: Vec<u8>,
}

impl Reply {
    pub(crate) fn status(&self) -> u32 {
        self.header.status
    }

    fn body(&self) -> &[u8] {
        &self.raw[HEADER_SIZE..]
    }

    fn checked(self, request: &str) -> TestResult<Self> {
        match self.status() {
            status::SUCCESS => Ok(self),
            other => Err(format!("SMB {request} failed with status {other:#010x}").into()),
        }
    }

    fn file_id(&self) -> TestResult<FileId> {
        let mut reader = wire::Reader::new(&self.body()[64..80]);
        Ok(FileId::read(&mut reader)?)
    }

    /// The security buffer of a SESSION_SETUP response.
    fn security(&self) -> Vec<u8> {
        let (start, length) = (half(self.body(), 4), half(self.body(), 6));
        self.raw[start..start + length].to_vec()
    }

    /// The data of a READ response at its own data offset.
    fn data(&self) -> Vec<u8> {
        let start = usize::from(self.body()[2]);
        let length = word(self.body(), 4) as usize;
        self.raw[start..start + length].to_vec()
    }
}

/// MD4 of RFC 1320, the NT one-way function over the UTF-16LE secret.
fn md4(input: &[u8]) -> [u8; 16] {
    let mut state = [0x6745_2301u32, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476];
    let mut message = input.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&((input.len() as u64) << 3).to_le_bytes());
    let f = |x: u32, y: u32, z: u32| (x & y) | (!x & z);
    let g = |x: u32, y: u32, z: u32| (x & y) | (x & z) | (y & z);
    let h = |x: u32, y: u32, z: u32| x ^ y ^ z;
    for block in message.chunks(64) {
        let x: Vec<u32> = block.chunks(4).map(|four| word(four, 0)).collect();
        let [mut a, mut b, mut c, mut d] = state;
        for i in [0, 4, 8, 12] {
            a = a.wrapping_add(f(b, c, d)).wrapping_add(x[i]).rotate_left(3);
            d = d
                .wrapping_add(f(a, b, c))
                .wrapping_add(x[i + 1])
                .rotate_left(7);
            c = c
                .wrapping_add(f(d, a, b))
                .wrapping_add(x[i + 2])
                .rotate_left(11);
            b = b
                .wrapping_add(f(c, d, a))
                .wrapping_add(x[i + 3])
                .rotate_left(19);
        }
        let two = 0x5a82_7999u32;
        for i in 0..4 {
            a = a
                .wrapping_add(g(b, c, d).wrapping_add(x[i]).wrapping_add(two))
                .rotate_left(3);
            d = d
                .wrapping_add(g(a, b, c).wrapping_add(x[i + 4]).wrapping_add(two))
                .rotate_left(5);
            c = c
                .wrapping_add(g(d, a, b).wrapping_add(x[i + 8]).wrapping_add(two))
                .rotate_left(9);
            b = b
                .wrapping_add(g(c, d, a).wrapping_add(x[i + 12]).wrapping_add(two))
                .rotate_left(13);
        }
        let three = 0x6ed9_eba1u32;
        for i in [0, 2, 1, 3] {
            a = a
                .wrapping_add(h(b, c, d).wrapping_add(x[i]).wrapping_add(three))
                .rotate_left(3);
            d = d
                .wrapping_add(h(a, b, c).wrapping_add(x[i + 8]).wrapping_add(three))
                .rotate_left(9);
            c = c
                .wrapping_add(h(d, a, b).wrapping_add(x[i + 4]).wrapping_add(three))
                .rotate_left(11);
            b = b
                .wrapping_add(h(c, d, a).wrapping_add(x[i + 12]).wrapping_add(three))
                .rotate_left(15);
        }
        for (slot, value) in state.iter_mut().zip([a, b, c, d]) {
            *slot = slot.wrapping_add(value);
        }
    }
    let mut out = [0u8; 16];
    for (chunk, value) in out.chunks_mut(4).zip(state) {
        chunk.copy_from_slice(&value.to_le_bytes());
    }
    out
}

/// HMAC-MD5 of RFC 2104 over the concatenated `parts`; every key here is 16 bytes.
fn hmac_md5(key: &[u8], parts: &[&[u8]]) -> [u8; 16] {
    let mut padded = [0u8; 64];
    padded[..key.len()].copy_from_slice(key);
    let mut inner = md5::Context::new();
    inner.consume(padded.map(|byte| byte ^ 0x36));
    for part in parts {
        inner.consume(*part);
    }
    let mut outer = md5::Context::new();
    outer.consume(padded.map(|byte| byte ^ 0x5c));
    outer.consume(inner.finalize().0);
    outer.finalize().0
}

/// The RC4 keystream applied in place, which NTLM key exchange uses.
fn rc4(key: &[u8], data: &mut [u8]) {
    let mut table: Vec<u8> = (0..=255u8).collect();
    let mut j = 0u8;
    for i in 0..256 {
        j = j.wrapping_add(table[i]).wrapping_add(key[i % key.len()]);
        table.swap(i, usize::from(j));
    }
    let (mut i, mut j) = (0u8, 0u8);
    for byte in data {
        i = i.wrapping_add(1);
        j = j.wrapping_add(table[usize::from(i)]);
        table.swap(usize::from(i), usize::from(j));
        let index = table[usize::from(i)].wrapping_add(table[usize::from(j)]);
        *byte ^= table[usize::from(index)];
    }
}

fn ntlm_negotiate() -> Vec<u8> {
    let flags =
        ntlm::REQUIRED | ntlm::KEY_EXCHANGE | ntlm::REQUEST_TARGET | ntlm::NTLM | ntlm::ALWAYS_SIGN;
    let flags = flags | ntlm::SIGN | ntlm::SEAL | ntlm::VERSION;
    let mut out = ntlm::SIGNATURE.to_vec();
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&[0u8; 8]);
    out.extend_from_slice(&[0u8; 8]);
    out.extend_from_slice(&[10, 0, 0x61, 0x4a, 0, 0, 0, 15]);
    out
}

/// The target information of a CHALLENGE with MsvAvFlags announcing a MIC, and its timestamp.
fn challenge_info(challenge: &[u8]) -> (Vec<u8>, [u8; 8]) {
    let (length, start) = (half(challenge, 40), word(challenge, 44) as usize);
    let mut pairs = &challenge[start..start + length];
    let (mut info, mut timestamp) = (Vec::new(), [0u8; 8]);
    while half(pairs, 0) as u16 != ntlm::AV_END {
        let size = half(pairs, 2);
        if half(pairs, 0) as u16 == ntlm::AV_TIMESTAMP {
            timestamp.copy_from_slice(&pairs[4..12]);
        }
        info.extend_from_slice(&pairs[..4 + size]);
        pairs = &pairs[4 + size..];
    }
    info.extend_from_slice(&ntlm::AV_FLAGS.to_le_bytes());
    info.extend_from_slice(&4u16.to_le_bytes());
    info.extend_from_slice(&ntlm::MIC_PRESENT.to_le_bytes());
    info.extend_from_slice(&[0u8; 4]);
    (info, timestamp)
}

/// An NTLMv2 AUTHENTICATE answering `challenge` with a MIC, and the exported session key.
fn ntlm_authenticate(
    negotiate: &[u8],
    challenge: &[u8],
    user: &str,
    secret: &str,
) -> (Vec<u8>, [u8; 16]) {
    let domain = "WORKGROUP";
    let (info, timestamp) = challenge_info(challenge);
    let mut blob = vec![1u8, 1, 0, 0, 0, 0, 0, 0];
    blob.extend_from_slice(&timestamp);
    blob.extend_from_slice(&ulid::Ulid::generate().to_bytes()[..8]);
    blob.extend_from_slice(&[0u8; 4]);
    blob.extend_from_slice(&info);
    blob.extend_from_slice(&[0u8; 4]);
    let identity = utf16_bytes(&format!("{}{domain}", user.to_uppercase()));
    let key = hmac_md5(&md4(&utf16_bytes(secret)), &[&identity]);
    let proof = hmac_md5(&key, &[&challenge[24..32], &blob]);
    let exported: [u8; 16] = ulid::Ulid::generate().to_bytes();
    let mut sealed_key = exported;
    rc4(&hmac_md5(&key, &[&proof]), &mut sealed_key);
    let payloads = [
        vec![0u8; 24],
        [proof.as_slice(), blob.as_slice()].concat(),
        utf16_bytes(domain),
        utf16_bytes(user),
        utf16_bytes("TESTCLIENT"),
        sealed_key.to_vec(),
    ];
    let mut out = ntlm::SIGNATURE.to_vec();
    out.extend_from_slice(&3u32.to_le_bytes());
    let mut at = 88u32;
    for payload in &payloads {
        let size = payload.len() as u16;
        out.extend_from_slice(&[size.to_le_bytes(), size.to_le_bytes()].concat());
        out.extend_from_slice(&at.to_le_bytes());
        at += u32::from(size);
    }
    out.extend_from_slice(&challenge[20..24]);
    out.extend_from_slice(&[10, 0, 0x61, 0x4a, 0, 0, 0, 15]);
    out.extend_from_slice(&[0u8; 16]);
    out.extend_from_slice(&payloads.concat());
    let mic = hmac_md5(&exported, &[negotiate, challenge, &out]);
    out[72..88].copy_from_slice(&mic);
    (out, exported)
}

/// A CREATE request body for `name` with every share mode and no create contexts.
fn create_body(name: &str, access: u32, disposition: u32, options: u32) -> Vec<u8> {
    let name = utf16_bytes(name);
    let body = Body::default().u16(57).u8(0).u8(0).u32(2).u64(0).u64(0);
    let body = body.u32(access).u32(0).u32(7).u32(disposition).u32(options);
    let body = body.u16(offset(56)).u16(name.len() as u16).u32(0).u32(0);
    body.tail(&name).0
}

/// One NEGOTIATE context: type, data length, reserved word and data.
fn context(kind: u16, data: &[u8]) -> Vec<u8> {
    let head = Body::default().u16(kind).u16(data.len() as u16).u32(0);
    [head.0, data.to_vec()].concat()
}

/// A 3.1.1-only NEGOTIATE with SHA-512 preauthentication, AES-128-GCM and AES-CMAC.
fn negotiate_body() -> Vec<u8> {
    let preauth = Body::default().u16(1).u16(32).u16(HASH_SHA512).0;
    let contexts = [
        context(1, &[preauth, vec![0x5a; 32]].concat()),
        context(2, &Body::default().u16(1).u16(CIPHER_AES128_GCM).0),
        context(8, &Body::default().u16(1).u16(SIGNING_CMAC).0),
    ];
    // The fixed part is 36 bytes and one dialect follows; contexts start 8-byte aligned.
    let first = (HEADER_SIZE + 38).next_multiple_of(8);
    let mut body = Body::default().u16(36).u16(1).u16(1).u16(0).u32(0x7f);
    body = body.u64(0x1111).u64(0x2222).u32(first as u32).u16(3).u16(0);
    let mut out = body.u16(DIALECT_311).0;
    for item in &contexts {
        while !(HEADER_SIZE + out.len()).is_multiple_of(8) {
            out.push(0);
        }
        out.extend_from_slice(item);
    }
    out
}

/// The cipher a NEGOTIATE response selected in its encryption context.
fn chosen_cipher(reply: &Reply) -> Option<u16> {
    let (count, mut at) = (half(reply.body(), 6), word(reply.body(), 60) as usize);
    for _ in 0..count {
        at = at.next_multiple_of(8);
        let (kind, length) = (half(&reply.raw, at), half(&reply.raw, at + 2));
        if kind == 2 {
            return Some(half(&reply.raw, at + 10) as u16);
        }
        at += 8 + length;
    }
    None
}

/// An SMB 3.1.1 client on one TCP connection; after SESSION_SETUP every request is sealed.
pub(crate) struct SmbClient {
    stream: TcpStream,
    message_id: u64,
    preauth: PreauthHash,
    session_id: u64,
    tree_id: u32,
    keys: Option<(SessionKeys, Sealer)>,
}

impl SmbClient {
    pub(crate) async fn connect(address: SocketAddr) -> TestResult<Self> {
        Ok(Self {
            stream: TcpStream::connect(address).await?,
            message_id: 0,
            preauth: PreauthHash::default(),
            session_id: 0,
            tree_id: 0,
            keys: None,
        })
    }

    /// Negotiates, logs in with the S3 key pair as NTLM user and secret and connects `share`.
    pub(crate) async fn login(
        address: SocketAddr,
        keys: &S3Credentials,
        share: &str,
    ) -> TestResult<Self> {
        let mut client = Self::connect(address).await?;
        client.negotiate().await?;
        let outcome = client
            .setup(&keys.access_key_id, &keys.access_secret)
            .await?;
        if outcome != status::SUCCESS {
            return Err(format!("SMB login failed with status {outcome:#010x}").into());
        }
        let path = utf16_bytes(&format!("\\\\127.0.0.1\\{share}"));
        let body = Body::default()
            .u16(9)
            .u16(0)
            .u16(offset(8))
            .u16(path.len() as u16);
        let reply = client
            .call(Command::TreeConnect, &body.tail(&path).0)
            .await?;
        client.tree_id = reply.checked("TREE_CONNECT")?.header.tree_id;
        Ok(client)
    }

    fn header(&mut self, command: Command) -> Vec<u8> {
        let header = Header {
            credit_charge: 1,
            command: command.code(),
            credits: 64,
            message_id: self.message_id,
            session_id: self.session_id,
            tree_id: self.tree_id,
            ..Header::default()
        };
        self.message_id += 1;
        let mut out = Vec::new();
        header.encode(&mut out);
        out
    }

    async fn send(&mut self, message: &[u8]) -> TestResult<()> {
        let packet = match self.keys.as_mut() {
            Some((_, sealer)) => sealer.seal(self.session_id, message)?,
            None => message.to_vec(),
        };
        self.stream.write_all(&wire::frame(&packet)?).await?;
        Ok(())
    }

    /// The next final response, or `None` once the server closed the connection.
    async fn receive(&mut self) -> TestResult<Option<Reply>> {
        loop {
            let read = wire::read_frame(&mut self.stream, wire::TRANSPORT_LIMIT).await;
            let Ok(Some(raw)) = read else {
                return Ok(None);
            };
            let raw = match (&self.keys, raw.starts_with(&wire::TRANSFORM_ID)) {
                (Some((keys, _)), true) => {
                    crypto::unseal(Cipher::Aes128Gcm, &keys.encryption, &raw)?
                }
                _ => raw,
            };
            let header = Header::decode(&raw)?;
            // A slow request first answers an interim STATUS_PENDING; its final answer follows.
            if header.status != status::PENDING || header.flags & wire::FLAG_ASYNC == 0 {
                return Ok(Some(Reply { header, raw }));
            }
        }
    }

    /// Sends one request and returns its final response, or `None` once the server closed.
    async fn request(&mut self, command: Command, body: &[u8]) -> TestResult<Option<Reply>> {
        let message = [self.header(command), body.to_vec()].concat();
        self.send(&message).await?;
        self.receive().await
    }

    async fn call(&mut self, command: Command, body: &[u8]) -> TestResult<Reply> {
        let reply = self.request(command, body).await?;
        reply.ok_or_else(|| format!("SMB server closed during {command:?}").into())
    }

    pub(crate) async fn negotiate(&mut self) -> TestResult<()> {
        let message = [self.header(Command::Negotiate), negotiate_body()].concat();
        self.preauth.update(&message);
        self.send(&message).await?;
        let reply = self.receive().await?.ok_or("no NEGOTIATE response")?;
        let reply = reply.checked("NEGOTIATE")?;
        self.preauth.update(&reply.raw);
        assert_eq!(
            half(reply.body(), 4) as u16,
            DIALECT_311,
            "SMB 3.1.1 is chosen"
        );
        assert_eq!(
            chosen_cipher(&reply),
            Some(CIPHER_AES128_GCM),
            "AES-128-GCM is chosen"
        );
        Ok(())
    }

    async fn setup_leg(&mut self, token: &[u8], hash: &mut PreauthHash) -> TestResult<Reply> {
        let body = Body::default().u16(25).u8(0).u8(1).u32(0).u32(0);
        let body = body
            .u16(offset(24))
            .u16(token.len() as u16)
            .u64(0)
            .tail(token);
        let message = [self.header(Command::SessionSetup), body.0].concat();
        hash.update(&message);
        self.send(&message).await?;
        let reply = self.receive().await?.ok_or("no SESSION_SETUP response")?;
        self.session_id = reply.header.session_id;
        if reply.status() == status::MORE_PROCESSING {
            hash.update(&reply.raw);
        }
        Ok(reply)
    }

    /// Runs raw NTLMSSP SESSION_SETUP and returns its final status; success keeps the keys.
    pub(crate) async fn setup(&mut self, user: &str, secret: &str) -> TestResult<u32> {
        let mut hash = self.preauth.clone();
        let negotiate = ntlm_negotiate();
        let first = self.setup_leg(&negotiate, &mut hash).await?;
        if first.status() != status::MORE_PROCESSING {
            return Ok(first.status());
        }
        let challenge = first.security();
        let (token, exported) = ntlm_authenticate(&negotiate, &challenge, user, secret);
        let last = self.setup_leg(&token, &mut hash).await?;
        if last.status() != status::SUCCESS {
            return Ok(last.status());
        }
        let keys = SessionKeys::derive(&exported, hash.value(), Cipher::Aes128Gcm)?;
        let signed = crypto::verify(Signing::AesCmac, &keys.signing, &last.raw)?;
        assert!(
            signed,
            "the final SESSION_SETUP response is signed with the derived key"
        );
        let sealer = Sealer::new(Cipher::Aes128Gcm, keys.decryption.clone());
        self.keys = Some((keys, sealer));
        Ok(status::SUCCESS)
    }

    /// One CREATE with read, write and delete sharing and no create contexts.
    pub(crate) async fn create(
        &mut self,
        name: &str,
        access: u32,
        disposition: u32,
        options: u32,
    ) -> TestResult<Reply> {
        let body = create_body(name, access, disposition, options);
        self.call(Command::Create, &body).await
    }

    /// Opens and closes the share root: its CREATE status, or `None` once the server closed.
    pub(crate) async fn probe(&mut self) -> TestResult<Option<u32>> {
        let body = create_body("", READ_ATTRIBUTES, FILE_OPEN, DIRECTORY);
        let Some(reply) = self.request(Command::Create, &body).await? else {
            return Ok(None);
        };
        if reply.status() == status::SUCCESS {
            self.close(reply.file_id()?).await?;
        }
        Ok(Some(reply.status()))
    }

    /// Opens or creates `name` and fails unless the server grants it.
    pub(crate) async fn open(
        &mut self,
        name: &str,
        access: u32,
        disposition: u32,
        options: u32,
    ) -> TestResult<FileId> {
        let reply = self.create(name, access, disposition, options).await?;
        reply.checked(&format!("CREATE {name}"))?.file_id()
    }

    pub(crate) async fn write(&mut self, file: FileId, at: u64, data: &[u8]) -> TestResult<()> {
        let body = Body::default()
            .u16(49)
            .u16(offset(48))
            .u32(data.len() as u32)
            .u64(at);
        let body = body.file(file).u32(0).u32(0).u16(0).u16(0).u32(0);
        let reply = self.call(Command::Write, &body.tail(data).0).await?;
        let reply = reply.checked("WRITE")?;
        assert_eq!(
            word(reply.body(), 4) as usize,
            data.len(),
            "WRITE takes every byte"
        );
        Ok(())
    }

    /// Writes `data` from offset zero in WRITE requests of at most `IO_CHUNK` bytes.
    pub(crate) async fn write_all(&mut self, file: FileId, data: &[u8]) -> TestResult<()> {
        for (index, chunk) in data.chunks(IO_CHUNK).enumerate() {
            self.write(file, (index * IO_CHUNK) as u64, chunk).await?;
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self, file: FileId) -> TestResult<()> {
        let body = Body::default().u16(24).u16(0).u32(0).file(file);
        self.call(Command::Flush, &body.0).await?.checked("FLUSH")?;
        Ok(())
    }

    /// One READ of `length` bytes at `at`, whatever its status.
    pub(crate) async fn read_at(
        &mut self,
        file: FileId,
        at: u64,
        length: u32,
    ) -> TestResult<Reply> {
        let body = Body::default().u16(49).u8(0x50).u8(0).u32(length).u64(at);
        let body = body.file(file).u32(0).u32(0).u32(0).u16(0).u16(0).u8(0);
        self.call(Command::Read, &body.0).await
    }

    /// Reads `size` bytes from offset zero in READ requests of at most `IO_CHUNK` bytes.
    pub(crate) async fn read_all(&mut self, file: FileId, size: usize) -> TestResult<Vec<u8>> {
        let mut out = Vec::with_capacity(size);
        while out.len() < size {
            let length = (size - out.len()).min(IO_CHUNK) as u32;
            let reply = self.read_at(file, out.len() as u64, length).await?;
            out.extend_from_slice(&reply.checked("READ")?.data());
        }
        Ok(out)
    }

    /// Sends one LOCK request for `range` without waiting and returns its MessageId.
    pub(crate) async fn lock_start(
        &mut self,
        file: FileId,
        range: (u64, u64),
        flags: u32,
    ) -> TestResult<u64> {
        let message_id = self.message_id;
        let message = [self.header(Command::Lock), lock_body(file, range, flags)].concat();
        self.send(&message).await?;
        Ok(message_id)
    }

    /// Locks or unlocks `range` and returns the final status.
    pub(crate) async fn lock(
        &mut self,
        file: FileId,
        range: (u64, u64),
        flags: u32,
    ) -> TestResult<u32> {
        self.lock_start(file, range, flags).await?;
        let answer = self.answer().await?;
        answer.ok_or_else(|| "SMB server closed during LOCK".into())
    }

    /// The status of the next final response, or `None` once the server closed.
    pub(crate) async fn answer(&mut self) -> TestResult<Option<u32>> {
        Ok(self.receive().await?.map(|reply| reply.status()))
    }

    /// Sends an unsealed CANCEL for `message_id` that names this encrypted session.
    pub(crate) async fn cancel_plain(&mut self, message_id: u64) -> TestResult<()> {
        let header = Header {
            command: Command::Cancel.code(),
            message_id,
            session_id: self.session_id,
            tree_id: self.tree_id,
            ..Header::default()
        };
        let mut message = Vec::new();
        header.encode(&mut message);
        let message = Body(message).u16(4).u16(0).0;
        self.stream.write_all(&wire::frame(&message)?).await?;
        Ok(())
    }

    async fn set_info(&mut self, file: FileId, class: u8, buffer: &[u8]) -> TestResult<Reply> {
        let body = Body::default()
            .u16(33)
            .u8(FILE_INFO)
            .u8(class)
            .u32(buffer.len() as u32);
        let body = body.u16(offset(32)).u16(0).u32(0).file(file);
        self.call(Command::SetInfo, &body.tail(buffer).0).await
    }

    /// Renames the open `file` to `target`, a backslash path from the share root.
    pub(crate) async fn rename(
        &mut self,
        file: FileId,
        target: &str,
        replace: bool,
    ) -> TestResult<()> {
        let name = utf16_bytes(target);
        let mut buffer = Body::default()
            .u8(u8::from(replace))
            .u8(0)
            .u16(0)
            .u32(0)
            .u64(0);
        buffer = buffer.u32(name.len() as u32).tail(&name);
        buffer
            .0
            .resize(buffer.0.len().max(aruna_smb::rename::MINIMUM_SIZE), 0);
        let reply = self.set_info(file, RENAME_CLASS, &buffer.0).await?;
        reply.checked(&format!("rename to {target}"))?;
        Ok(())
    }

    /// Marks the open `file` delete-pending, so its last close removes it.
    pub(crate) async fn dispose(&mut self, file: FileId) -> TestResult<()> {
        let reply = self.set_info(file, DISPOSITION_CLASS, &[1]).await?;
        reply.checked("delete disposition")?;
        Ok(())
    }

    pub(crate) async fn close(&mut self, file: FileId) -> TestResult<()> {
        let body = Body::default().u16(24).u16(0).u32(0).file(file);
        self.call(Command::Close, &body.0).await?.checked("CLOSE")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 1320 test suite values and the MS-NLMP NT hash of "Password".
    #[test]
    fn md4_vectors() {
        assert_eq!(hex::encode(md4(b"")), "31d6cfe0d16ae931b73c59d7e0c089c0");
        assert_eq!(hex::encode(md4(b"abc")), "a448017aaf21d8525fc10ae87aa6729d");
        let digits = "1234567890".repeat(8);
        assert_eq!(
            hex::encode(md4(digits.as_bytes())),
            "e33b4ddc9c38f2199c3e7b164fcc0536"
        );
        let hash = md4(&utf16_bytes("Password"));
        assert_eq!(hex::encode(hash), "a4f49c406510bdcab6824ee7c30fd852");
    }

    #[test]
    fn rc4_hmac_vectors() {
        let mut data = b"Plaintext".to_vec();
        rc4(b"Key", &mut data);
        assert_eq!(hex::encode(&data), "bbf316e8d940af0ad3");
        let mac = hmac_md5(b"Jefe", &[b"what do ya want ", b"for nothing?"]);
        assert_eq!(hex::encode(mac), "750c783e6ab0b503eaa86e310a5db738");
    }
}
