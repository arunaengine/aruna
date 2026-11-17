//! Owns the forwarding parts shared by inbound adapters: peer auth, transport, replay, routing,
//! and the forwarded token revokes and realm admin relays.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod authorize;
pub mod fetch;
pub mod realm;
pub mod replay;
pub mod routing;
pub mod token;
pub mod transport;
