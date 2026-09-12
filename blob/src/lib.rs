#![allow(clippy::result_large_err)]
#![recursion_limit = "256"]

//! Blob storage backends and the effect-driven blob handle.
mod autoindex;
pub mod bao_tree;
pub mod blob;
pub mod egress;
pub mod error;
mod framing;
mod fs_source;
mod fs_write;
pub mod hash;
mod messages;
pub mod opendal;
pub mod s3;
