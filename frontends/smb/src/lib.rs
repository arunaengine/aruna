//! Embedded SMB 3.1.1 server: SHA-512 preauthentication, AES-GCM encryption and NTLMv2 over
//! SPNEGO, serving local buckets and job input views through an `SmbBackend`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

pub mod access;
pub mod aruna;
pub mod backend;
pub mod crypto;
mod dispatch;
mod handshake;
pub mod info;
pub mod messages;
pub mod ntlm;
pub mod path;
pub mod rename;
mod server;
mod session;
pub mod spnego;
pub mod status;
#[cfg(test)]
mod testing;
pub mod wire;

pub use aruna::{ArunaSmbBackend, ArunaSmbConfig};
pub use backend::{
    BackendError, BasicChange, CreateAction, Credential, CredentialSecret, DirectoryEntry,
    Disposition, FileInfo, InfoChange, ListPage, LockRange, Lookup, ObjectKind, OpenGrant,
    OpenRequest, Revocation, Rights, Scope, ShareTarget, SmbBackend, TreeGrant, WriteMode,
    WriteRequest,
};
pub use ntlm::MicPolicy;
pub use server::{SmbConfig, SmbServer, SmbServerError, SmbServerHandle};
