// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
#![allow(clippy::result_large_err)]

pub mod bootstrap;
pub mod config;
pub mod default_env;
pub mod portal;
pub mod shutdown;
pub mod telemetry;
