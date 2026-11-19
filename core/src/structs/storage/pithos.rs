//! Size and memory bounds of Pithos archives, shared by the blob backend that writes them and
//! the operations that refuse a write before it starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Largest original object a Pithos copy holds, about 0.93 TiB: a re-encode reserves the read and
/// the write of such an object together, and that still fits the node budget.
pub const MAX_SIZE: u64 = ((WORKING_SET / 2 - BASE_SHARE) / BLOCK_MEMORY - MAX_PIECES) * MIN_BLOCK;
/// Pithos metadata, encoder state and read buffers that may be in use at once on one node.
pub const WORKING_SET: u64 = 2 << 30;
/// Fixed share of one open, encoder or composition: batches, sealed blocks and read buffers.
pub const BASE_SHARE: u64 = 64 << 20;
/// Smallest block of an Aruna archive except the last block of each piece.
pub const MIN_BLOCK: u64 = 1 << 20;
/// Pieces of one archive: the S3 part numbers 1 to 10,000.
pub const MAX_PIECES: u64 = 10_000;
/// Memory per block while its encoded directory entry, decoded descriptor and index entries are
/// held at the same time; the measured allocation test checks this bound.
pub const BLOCK_MEMORY: u64 = 1024;
