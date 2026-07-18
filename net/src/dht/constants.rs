use std::time::Duration;

pub const CMD_CHANNEL_CAPACITY: usize = 1024;
pub const DRIVER_IO_EVENT_CAPACITY: usize = 2048;
pub const INBOUND_STREAM_CAPACITY: usize = 256;

pub const LOOKUP_ALPHA: usize = 3;
pub const LOOKUP_MAX_QUERIES: usize = 64;

pub const MAX_TTL_SECS: u64 = 24 * 60 * 60;
pub const MAX_CLOCK_SKEW_SECS: u64 = 5 * 60;
pub const MAX_VALUE_SIZE: usize = 48 * 1024;
pub const MAX_ENTRIES_PER_KEY: usize = 256;
pub const MAX_STORED_VALUE_SIZE: usize = 56 * 1024;
pub const MAX_STORED_KEYS: u64 = 65_536;
pub const STORAGE_MUTATION_RETRIES: usize = 8;

pub const DHT_META_KEYSPACE: &str = "dht_meta_v2";
pub const DHT_KEY_COUNT_KEY: &[u8] = b"key_count";
pub const DHT_REVISION_KEY: &[u8] = b"revision";

pub const DRIVER_TICK_INTERVAL: Duration = Duration::from_secs(30);
pub const RPC_TIMEOUT: Duration = Duration::from_secs(10);
pub const STORAGE_TIMEOUT: Duration = Duration::from_secs(10);
pub const RPC_TIMEOUT_TICKS: u64 = 4;

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024;
