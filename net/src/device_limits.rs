//! Per-device inbound limits.
//!
//! A user device is the one realm key whose request volume is not implicitly
//! bounded by realm responsibility: it holds nothing, serves nothing, and only
//! asks. Its inbound streams are therefore charged against a per-node budget the
//! realm configuration publishes, at the same admission point that already
//! applies the ALPN by kind boundary. Realm nodes are never charged here.

use aruna_core::NodeId;
use governor::clock::{Clock, DefaultClock};
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Devices tracked at once. An idle device is dropped from the table before the
/// cap refuses a new one, so a busy realm never loses its own limits.
const DEVICE_TABLE_LIMIT: usize = 4_096;
/// Every N charges the rate table drops fully replenished devices.
const MAINTENANCE_INTERVAL: u64 = 1_024;

type Keyed = RateLimiter<NodeId, DefaultKeyedStateStore<NodeId>, DefaultClock>;

/// What one user device may ask of this node. `None` leaves that dimension
/// uncapped, which is the realm default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeviceLimits {
    pub requests_per_minute: Option<u32>,
    pub concurrent: Option<u32>,
}

/// Why an inbound device stream was not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceRefusal {
    /// Over the configured per-minute budget; the value names the seconds after
    /// which a retry can conform and is never zero.
    Rate(u64),
    /// Every concurrent slot this device is allowed is already in use.
    Concurrency,
}

/// Held for as long as the admitted stream is served, so a device's concurrent
/// slot is released exactly when its request finishes.
#[derive(Debug)]
pub struct DevicePermit {
    _permit: Option<OwnedSemaphorePermit>,
}

#[derive(Default)]
struct LimiterState {
    limits: DeviceLimits,
    rate: Option<Arc<Keyed>>,
    in_flight: HashMap<NodeId, Arc<Semaphore>>,
}

#[derive(Default)]
pub struct DeviceLimiter {
    state: Mutex<LimiterState>,
    clock: DefaultClock,
    charges: AtomicU64,
}

impl std::fmt::Debug for DeviceLimiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DeviceLimiter").finish()
    }
}

impl DeviceLimiter {
    /// Publishes the realm's current device limits. Only a changed value rebuilds
    /// its budget, so a configuration refresh that changes nothing else never
    /// resets the limits already being applied.
    pub fn configure(&self, limits: DeviceLimits) {
        let mut state = self.state.lock();
        if state.limits == limits {
            return;
        }
        if state.limits.requests_per_minute != limits.requests_per_minute {
            state.rate = limits.requests_per_minute.map(|per_minute| {
                Arc::new(RateLimiter::keyed(Quota::per_minute(
                    NonZeroU32::new(per_minute).unwrap_or(NonZeroU32::MIN),
                )))
            });
        }
        if state.limits.concurrent != limits.concurrent {
            state.in_flight.clear();
        }
        state.limits = limits;
    }

    /// Charges one inbound stream from `device`. The permit releases the
    /// concurrent slot when it is dropped.
    pub fn admit(&self, device: NodeId) -> Result<DevicePermit, DeviceRefusal> {
        let (rate, semaphore) = self.reserve(device);
        if let Some(rate) = rate {
            if self
                .charges
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(MAINTENANCE_INTERVAL)
            {
                rate.retain_recent();
            }
            let now = self.clock.now();
            rate.check_key(&device).map_err(|not_until| {
                DeviceRefusal::Rate(not_until.wait_time_from(now).as_secs().max(1))
            })?;
        }
        let Some(semaphore) = semaphore else {
            return Ok(DevicePermit { _permit: None });
        };
        semaphore
            .try_acquire_owned()
            .map(|permit| DevicePermit {
                _permit: Some(permit),
            })
            .map_err(|_| DeviceRefusal::Concurrency)
    }

    /// The budgets this charge runs against. A device the table has no room for
    /// is admitted uncharged on the concurrency dimension, because an unbounded
    /// table is the worse failure.
    fn reserve(&self, device: NodeId) -> (Option<Arc<Keyed>>, Option<Arc<Semaphore>>) {
        let mut state = self.state.lock();
        let Some(concurrent) = state.limits.concurrent else {
            return (state.rate.clone(), None);
        };
        if let Some(semaphore) = state.in_flight.get(&device) {
            return (state.rate.clone(), Some(semaphore.clone()));
        }
        if state.in_flight.len() >= DEVICE_TABLE_LIMIT {
            let idle = concurrent as usize;
            state
                .in_flight
                .retain(|_, semaphore| semaphore.available_permits() < idle);
        }
        let semaphore = match state.in_flight.len() >= DEVICE_TABLE_LIMIT {
            true => None,
            false => {
                let semaphore = Arc::new(Semaphore::new(concurrent as usize));
                state.in_flight.insert(device, semaphore.clone());
                Some(semaphore)
            }
        };
        (state.rate.clone(), semaphore)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn uncapped_by_default() {
        // A realm that configured nothing charges its devices nothing.
        let limiter = DeviceLimiter::default();
        for _ in 0..64 {
            assert!(limiter.admit(device(1)).is_ok());
        }
    }

    #[test]
    fn limits_by_device() {
        // The per-minute budget drains for one device and names a retry delay,
        // while every other device keeps its own budget.
        let limiter = DeviceLimiter::default();
        limiter.configure(DeviceLimits {
            requests_per_minute: Some(2),
            concurrent: None,
        });

        assert!(limiter.admit(device(1)).is_ok());
        assert!(limiter.admit(device(1)).is_ok());
        assert!(matches!(
            limiter.admit(device(1)),
            Err(DeviceRefusal::Rate(seconds)) if seconds >= 1
        ));
        assert!(limiter.admit(device(2)).is_ok());
    }

    #[test]
    fn limits_in_flight() {
        // Concurrent slots bound long transfers and come back when the stream
        // holding one finishes.
        let limiter = DeviceLimiter::default();
        limiter.configure(DeviceLimits {
            requests_per_minute: None,
            concurrent: Some(2),
        });

        let first = limiter.admit(device(1)).expect("within the limit");
        let second = limiter.admit(device(1)).expect("within the limit");
        assert!(matches!(
            limiter.admit(device(1)),
            Err(DeviceRefusal::Concurrency)
        ));
        assert!(limiter.admit(device(2)).is_ok());

        drop(second);
        assert!(limiter.admit(device(1)).is_ok());
        drop(first);
    }

    #[test]
    fn reconfigures_budgets() {
        // A raised limit applies without a restart, and clearing it leaves the
        // devices uncharged again.
        let limiter = DeviceLimiter::default();
        limiter.configure(DeviceLimits {
            requests_per_minute: None,
            concurrent: Some(1),
        });
        let held = limiter.admit(device(1)).expect("within the limit");
        assert!(matches!(
            limiter.admit(device(1)),
            Err(DeviceRefusal::Concurrency)
        ));

        limiter.configure(DeviceLimits {
            requests_per_minute: None,
            concurrent: Some(2),
        });
        assert!(limiter.admit(device(1)).is_ok());

        limiter.configure(DeviceLimits::default());
        for _ in 0..8 {
            assert!(limiter.admit(device(1)).is_ok());
        }
        drop(held);
    }
}
