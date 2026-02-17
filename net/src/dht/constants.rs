use std::time::Duration;

pub const CMD_CHANNEL_CAPACITY: usize = 1024;
pub const DRIVER_IO_REQUEST_CAPACITY: usize = 2048;
pub const DRIVER_IO_EVENT_CAPACITY: usize = 2048;
pub const INBOUND_STREAM_CAPACITY: usize = 256;

pub const DRIVER_STEP_BUDGET: usize = 256;

pub const DRIVER_TICK_INTERVAL: Duration = Duration::from_secs(30);
pub const RPC_TIMEOUT: Duration = Duration::from_secs(10);
pub const RPC_TIMEOUT_TICKS: u64 = 4;

pub const MAX_MESSAGE_SIZE: usize = 64 * 1024;
