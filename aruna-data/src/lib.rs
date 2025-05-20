pub const ARUNA_NET_ALPN: &[u8] = b"aruna-net/0.1.0";

// Module declarations
pub mod config;
pub mod io;
pub mod util;

pub use io::io_handler::IOHandler;
