//! Embedded NFSv4.1 server over RFC 9289 RPC-over-TLS (`sunrpc` ALPN, mutual TLS) serving
//! Aruna local buckets, scoped folders and immutable job input views through `aruna-runner`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

mod arguments;
pub mod aruna;
mod attributes;
pub mod backend;
mod compound;
pub mod filehandle;
mod files;
mod opens;
pub mod record;
pub mod rpc;
mod server;
mod session;
mod state;
pub mod tls;
pub mod types;
pub mod xdr;

#[cfg(test)]
mod mock;

pub use aruna::{ArunaNfsBackend, ArunaNfsConfig};
pub use backend::{
    AttributeChange, Caller, ChangeInfo, CreateMode, CreateReply, DirectoryEntry, DirectoryPage,
    HandleStatus, HostIdentity, IoState, LockDenial, LockError, LockKind, LockRange, MountExport,
    NfsBackend, ObjectAttributes, ObjectRef, OwnerKey, ReadReply, ReplayGrant, Revocation, SetTime,
    ShareRequest, WriteReply,
};
pub use rpc::{DisabledGssAcceptor, GssAcceptor};
pub use server::{NfsServer, NfsServerConfig, NfsServerError, NfsServerHandle};
pub use tls::{NfsTlsConfig, TlsSetupError};
pub use types::{NfsStat4, NfsTime, NfsType4, StableHow};
