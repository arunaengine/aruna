//! Calls this node passes on to another node of the realm: what is sent, to which node, and what
//! the answer means. The HTTP itself runs in the outbound backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod call;
pub mod hook;
pub mod management;
pub mod targets;
pub mod tool;
pub mod tool_route;
