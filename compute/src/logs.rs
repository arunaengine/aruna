use std::collections::VecDeque;

/// Which stream a chunk belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Optional full-stream copy target invoked for every captured chunk, in
/// addition to the bounded tail. Backends call this before dropping bytes.
pub trait LogSink: Send + Sync {
    fn write(&self, stream: LogStream, chunk: &[u8]);
}

/// A sink that discards everything (bounded tail only).
pub struct NullSink;

impl LogSink for NullSink {
    fn write(&self, _stream: LogStream, _chunk: &[u8]) {}
}

/// Bounded tail captured per stream plus the true totals so truncation is
/// observable rather than silent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogTails {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_total: u64,
    pub stderr_total: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

/// Fixed-capacity ring retaining the last `cap` bytes seen while counting the
/// total, so a chatty stream stays bounded.
#[derive(Debug)]
pub struct BoundedTail {
    cap: usize,
    buf: VecDeque<u8>,
    total: u64,
}

impl BoundedTail {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            buf: VecDeque::new(),
            total: 0,
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.total += data.len() as u64;
        if self.cap == 0 {
            return;
        }
        // Only the last `cap` bytes of `data` can survive.
        let tail = if data.len() > self.cap {
            &data[data.len() - self.cap..]
        } else {
            data
        };
        self.buf.extend(tail.iter().copied());
        while self.buf.len() > self.cap {
            self.buf.pop_front();
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn truncated(&self) -> bool {
        self.total > self.buf.len() as u64
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_bounds() {
        // Retains only the last `cap` bytes while counting the full total.
        let mut tail = BoundedTail::new(4);
        tail.push(b"abcdefgh");
        tail.push(b"ij");
        assert_eq!(tail.total(), 10);
        assert!(tail.truncated());
        assert_eq!(tail.into_bytes(), b"ghij");
    }

    #[test]
    fn tail_exact() {
        // Below the cap nothing is dropped and truncation stays false.
        let mut tail = BoundedTail::new(16);
        tail.push(b"hello");
        assert!(!tail.truncated());
        assert_eq!(tail.total(), 5);
        assert_eq!(tail.into_bytes(), b"hello");
    }
}
