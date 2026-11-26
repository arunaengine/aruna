//! NEGOTIATE limited to dialect 3.1.1 with SHA-512 preauthentication and AES-GCM, and
//! SESSION_SETUP over SPNEGO or raw NTLMSSP ending in transcript-bound session keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::backend::{Credential, SmbBackend};
use crate::crypto::{Cipher, HASH_SHA512, PreauthHash, Sealer, SessionKeys, Signing};
use crate::dispatch::{Element, Reply};
use crate::info::filetime;
use crate::messages::{self, NegotiateContext, NegotiateReply};
use crate::ntlm::{self, Challenge, SessionSecurity, TargetNames};
use crate::server::{Connection, Fatal, random_bytes};
use crate::session::{Mechanism, Negotiated, Ready, Session, Stage};
use crate::spnego::{self, ClientToken, NTLMSSP_OID};
use crate::status;

pub(crate) const DIALECT: u16 = 0x0311;
/// SMB2_NEGOTIATE_SIGNING_ENABLED | SMB2_NEGOTIATE_SIGNING_REQUIRED.
pub(crate) const SECURITY_MODE: u16 = 0x0003;
/// SMB2_GLOBAL_CAP_LARGE_MTU only: no leasing, no multichannel, no persistent handles.
pub(crate) const SERVER_CAPABILITIES: u32 = 0x0000_0004;
const PREAUTH_CONTEXT: u16 = 0x0001;
const ENCRYPTION_CONTEXT: u16 = 0x0002;
const SIGNING_CONTEXT: u16 = 0x0008;
const SESSION_ENCRYPT: u16 = 0x0004;
const SESSION_BINDING: u8 = 0x01;
const SALT_SIZE: usize = 32;

/// The algorithm list of a negotiate context; preauthentication lists follow a salt length.
fn algorithm_list(data: &[u8], salted: bool) -> Option<Vec<u16>> {
    let count = u16::from_le_bytes([*data.first()?, *data.get(1)?]) as usize;
    let start = if salted { 4 } else { 2 };
    let bytes = data.get(start..start + count * 2)?;
    Some(
        bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_le_bytes(*pair))
            .collect(),
    )
}

fn context(kind: u16, values: &[u16], salt: Option<&[u8]>) -> NegotiateContext {
    let mut data = (values.len() as u16).to_le_bytes().to_vec();
    if let Some(salt) = salt {
        data.extend_from_slice(&(salt.len() as u16).to_le_bytes());
    }
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    if let Some(salt) = salt {
        data.extend_from_slice(salt);
    }
    NegotiateContext { kind, data }
}

impl<B: SmbBackend> Connection<B> {
    /// Accepts only dialect 3.1.1 with SHA-512 preauthentication and an AES-GCM cipher. A
    /// non-success reply is sent and then the connection closes.
    pub(crate) fn negotiate(&self, element: &Element) -> Result<Reply, Fatal> {
        if self.state.lock().negotiated.is_some() {
            return Err(Fatal("second NEGOTIATE"));
        }
        self.shared
            .limits
            .check_handshake(self.peer)
            .map_err(|_| Fatal("handshake rate limit"))?;
        let request =
            messages::parse_negotiate(&element.bytes).map_err(|_| Fatal("malformed NEGOTIATE"))?;
        if !request.dialects.contains(&DIALECT) {
            return Ok(Reply::fail(status::NOT_SUPPORTED));
        }
        let (mut hashes, mut ciphers, mut signings) = (None, None, None);
        for item in &request.contexts {
            let slot = match item.kind {
                PREAUTH_CONTEXT => &mut hashes,
                ENCRYPTION_CONTEXT => &mut ciphers,
                SIGNING_CONTEXT => &mut signings,
                _ => continue,
            };
            if slot.is_some() {
                return Ok(Reply::fail(status::INVALID_PARAMETER));
            }
            let Some(list) = algorithm_list(&item.data, item.kind == PREAUTH_CONTEXT) else {
                return Ok(Reply::fail(status::INVALID_PARAMETER));
            };
            *slot = Some(list);
        }
        let Some(hashes) = hashes else {
            return Ok(Reply::fail(status::INVALID_PARAMETER));
        };
        if !hashes.contains(&HASH_SHA512) {
            return Ok(Reply::fail(status::NOT_SUPPORTED));
        }
        let Some(cipher) = ciphers
            .unwrap_or_default()
            .into_iter()
            .find_map(Cipher::from_code)
        else {
            return Ok(Reply::fail(status::NOT_SUPPORTED));
        };
        let signing = signings
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find_map(|code| Signing::from_code(*code))
            .unwrap_or(Signing::AesCmac);
        let salt: [u8; SALT_SIZE] = random_bytes()?;
        let mut contexts = vec![
            context(PREAUTH_CONTEXT, &[HASH_SHA512], Some(salt.as_slice())),
            context(ENCRYPTION_CONTEXT, &[cipher.code()], None),
        ];
        if signings.is_some() {
            contexts.push(context(SIGNING_CONTEXT, &[signing.code()], None));
        }
        {
            let mut state = self.state.lock();
            state.preauth.update(&element.bytes);
            state.negotiated = Some(Negotiated {
                cipher,
                signing,
                client_guid: request.client_guid,
                client_mode: request.security_mode,
                client_capabilities: request.capabilities,
                dialects: request.dialects.clone(),
            });
        }
        let config = &self.shared.config;
        let hint = spnego::server_hint();
        let body = messages::negotiate_body(&NegotiateReply {
            security_mode: SECURITY_MODE,
            dialect: DIALECT,
            server_guid: self.shared.server_guid,
            capabilities: SERVER_CAPABILITIES,
            max_transact: config.max_transact,
            max_read: config.max_read,
            max_write: config.max_write,
            system_time: filetime(aruna_clock::unix_timestamp_millis()),
            security: &hint,
            contexts: &contexts,
        });
        Ok(Reply::ok(body))
    }

    /// One SESSION_SETUP leg. Every request enters the session's preauthentication hash
    /// before it is processed; a failure removes the session.
    pub(crate) async fn session_setup(self: &Arc<Self>, element: &Element) -> Result<Reply, Fatal> {
        let cipher = self
            .state
            .lock()
            .cipher()
            .ok_or(Fatal("SESSION_SETUP before NEGOTIATE"))?;
        let Ok(request) = messages::parse_setup(&element.bytes) else {
            return Ok(Reply::fail(status::INVALID_PARAMETER));
        };
        if request.flags & SESSION_BINDING != 0 {
            return Ok(Reply::fail(status::REQUEST_NOT_ACCEPTED));
        }
        let session_id = if element.header.session_id == 0 {
            self.shared
                .limits
                .check_handshake(self.peer)
                .map_err(|_| Fatal("handshake rate limit"))?;
            self.new_session()?
        } else {
            element.header.session_id
        };
        {
            let mut state = self.state.lock();
            match state
                .sessions
                .get_mut(&session_id)
                .map(|session| &mut session.stage)
            {
                Some(Stage::Handshake(handshake)) => handshake.preauth.update(&element.bytes),
                Some(Stage::Ready(_)) => return Ok(Reply::fail(status::NOT_SUPPORTED)),
                None => return Ok(Reply::fail(status::USER_SESSION_DELETED)),
            }
        }
        let mut reply = match self
            .authenticate(session_id, &request.security, cipher)
            .await
        {
            Ok(reply) => reply,
            Err(code) => {
                tracing::debug!(code, "SMB session setup refused");
                self.state.lock().remove_session(session_id);
                Reply::fail(code)
            }
        };
        reply.session_id = Some(session_id);
        Ok(reply)
    }

    fn new_session(&self) -> Result<u64, Fatal> {
        loop {
            let id = u64::from_le_bytes(random_bytes()?);
            let mut state = self.state.lock();
            if id != 0 && id != u64::MAX && !state.sessions.contains_key(&id) {
                let preauth = state.preauth.clone();
                state.sessions.insert(id, Session::new(preauth));
                return Ok(id);
            }
        }
    }

    async fn authenticate(
        self: &Arc<Self>,
        session_id: u64,
        security: &[u8],
        cipher: Cipher,
    ) -> Result<Reply, u32> {
        let token = spnego::parse_client(security).map_err(|_| status::LOGON_FAILURE)?;
        let (mechanism, challenge) = {
            let state = self.state.lock();
            match state
                .sessions
                .get(&session_id)
                .map(|session| &session.stage)
            {
                Some(Stage::Handshake(handshake)) => {
                    (handshake.mechanism.clone(), handshake.challenge.clone())
                }
                _ => return Err(status::USER_SESSION_DELETED),
            }
        };
        match challenge {
            None => self.challenge_leg(session_id, mechanism, token),
            Some(challenge) => {
                self.final_leg(session_id, mechanism, challenge, token, cipher)
                    .await
            }
        }
    }

    fn challenge_leg(
        &self,
        session_id: u64,
        mechanism: Option<Mechanism>,
        token: ClientToken,
    ) -> Result<Reply, u32> {
        let first = mechanism.is_none();
        let (mechanism, negotiate) = match (mechanism, token) {
            (None, ClientToken::Init(init)) => {
                if !init.mechanisms.iter().any(|item| item == NTLMSSP_OID) {
                    return Err(status::LOGON_FAILURE);
                }
                let preferred = init.mechanisms[0] == NTLMSSP_OID;
                let negotiate = if preferred { init.token } else { None };
                (
                    Mechanism::Spnego {
                        list: init.mechanism_list,
                        preferred,
                    },
                    negotiate,
                )
            }
            (None, ClientToken::Raw(bytes)) => (Mechanism::Raw, Some(bytes)),
            (Some(chosen @ Mechanism::Spnego { .. }), ClientToken::Response(response)) => {
                (chosen, response.token)
            }
            _ => return Err(status::LOGON_FAILURE),
        };
        let challenge = match negotiate {
            Some(negotiate) => {
                let names = TargetNames {
                    domain: self.shared.config.domain.clone(),
                    computer: self.shared.config.computer.clone(),
                };
                let server_challenge: [u8; 8] =
                    random_bytes().map_err(|_| status::INSUFFICIENT_RESOURCES)?;
                let timestamp = filetime(aruna_clock::unix_timestamp_millis());
                Some(
                    Challenge::answer(&negotiate, server_challenge, timestamp, &names)
                        .map_err(|_| status::LOGON_FAILURE)?,
                )
            }
            None if mechanism == Mechanism::Raw => return Err(status::LOGON_FAILURE),
            None => None,
        };
        let message = challenge.as_ref().map(|value| value.message().to_vec());
        let security = match &mechanism {
            Mechanism::Raw => message.unwrap_or_default(),
            Mechanism::Spnego { .. } => spnego::encode_response(
                Some(spnego::ACCEPT_INCOMPLETE),
                first,
                message.as_deref(),
                None,
            ),
        };
        {
            let mut state = self.state.lock();
            let Some(Session {
                stage: Stage::Handshake(handshake),
                ..
            }) = state.sessions.get_mut(&session_id)
            else {
                return Err(status::USER_SESSION_DELETED);
            };
            handshake.mechanism = Some(mechanism);
            handshake.challenge = challenge;
        }
        Ok(Reply::with(
            status::MORE_PROCESSING,
            messages::setup_body(0, &security),
        ))
    }

    async fn final_leg(
        self: &Arc<Self>,
        session_id: u64,
        mechanism: Option<Mechanism>,
        challenge: Challenge,
        token: ClientToken,
        cipher: Cipher,
    ) -> Result<Reply, u32> {
        let (bytes, list_mic) = match (&mechanism, token) {
            (Some(Mechanism::Raw), ClientToken::Raw(bytes)) => (bytes, None),
            (Some(Mechanism::Spnego { .. }), ClientToken::Response(response)) => (
                response.token.ok_or(status::LOGON_FAILURE)?,
                response.list_mic,
            ),
            _ => return Err(status::LOGON_FAILURE),
        };
        let auth = ntlm::parse_authenticate(&bytes).map_err(|_| status::LOGON_FAILURE)?;
        let found = self
            .backend
            .lookup_secret(&auth.user)
            .await
            .map_err(|_| status::LOGON_FAILURE)?;
        let policy = self.shared.config.mic_policy;
        let (verified, principal) = match found {
            Some(secret) => {
                let verified = challenge.verify(&auth, &secret.secret, policy);
                let principal = secret.principal;
                drop(secret);
                (verified.map_err(|_| status::LOGON_FAILURE)?, principal)
            }
            None => {
                let filler: [u8; 16] =
                    random_bytes().map_err(|_| status::INSUFFICIENT_RESOURCES)?;
                let decoy = Zeroizing::new(format!("{:032x}", u128::from_le_bytes(filler)));
                let _ = challenge.verify(&auth, &decoy, policy);
                return Err(status::LOGON_FAILURE);
            }
        };
        let server_mic = match &mechanism {
            Some(Mechanism::Spnego { list, preferred }) => {
                let mut security = SessionSecurity::new(&verified.session_key)
                    .map_err(|_| status::LOGON_FAILURE)?;
                let required = verified.mic_present || !preferred;
                match list_mic {
                    Some(mic) => {
                        let expected = security
                            .client_mic(0, list)
                            .map_err(|_| status::LOGON_FAILURE)?;
                        if !bool::from(expected.as_slice().ct_eq(&mic)) {
                            return Err(status::LOGON_FAILURE);
                        }
                        Some(
                            security
                                .server_mic(0, list)
                                .map_err(|_| status::LOGON_FAILURE)?,
                        )
                    }
                    None if required => return Err(status::LOGON_FAILURE),
                    None => None,
                }
            }
            _ => None,
        };
        let credential = Credential {
            access_key: auth.user.clone(),
            principal,
        };
        self.backend
            .admit(&credential)
            .await
            .map_err(|_| status::LOGON_FAILURE)?;
        let preauth: PreauthHash = {
            let state = self.state.lock();
            match state
                .sessions
                .get(&session_id)
                .map(|session| &session.stage)
            {
                Some(Stage::Handshake(handshake)) => handshake.preauth.clone(),
                _ => return Err(status::USER_SESSION_DELETED),
            }
        };
        let keys = SessionKeys::derive(verified.session_key.as_slice(), preauth.value(), cipher)
            .map_err(|_| status::LOGON_FAILURE)?;
        let sealer = Sealer::new(cipher, keys.encryption.clone());
        {
            let mut state = self.state.lock();
            let session = state
                .sessions
                .get_mut(&session_id)
                .ok_or(status::USER_SESSION_DELETED)?;
            session.stage = Stage::Ready(Box::new(Ready {
                credential,
                keys,
                sealer,
                expired: false,
            }));
        }
        let security = match mechanism {
            Some(Mechanism::Spnego { .. }) => spnego::encode_response(
                Some(spnego::ACCEPT_COMPLETED),
                false,
                None,
                server_mic.as_ref().map(|mic| mic.as_slice()),
            ),
            _ => Vec::new(),
        };
        let mut reply = Reply::ok(messages::setup_body(SESSION_ENCRYPT, &security));
        reply.signed = true;
        Ok(reply)
    }
}
