// Module declarations
pub mod api_json;
pub mod api_s3;
pub mod config;
pub mod error;
pub mod io;
pub mod util;

pub const ARUNA_NET_ALPN: &[u8] = b"aruna-net/0.1.0";

pub use crate::io::io_handler::tables::{
    ACCESS_DB_NAME, LOCATION_DB_NAME, LOCATION_STATS_DB_NAME, PATH_LOCATION_DB_NAME,
};
pub use io::io_handler::IOHandler;
