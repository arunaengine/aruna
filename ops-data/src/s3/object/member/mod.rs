//! Groups the object calls of a distributed bucket member: reads, writes, the rest and merges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod merge;
pub mod read;
pub mod rest;
pub mod write;
