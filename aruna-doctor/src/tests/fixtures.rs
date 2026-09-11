use std::sync::OnceLock;
use tokio::sync::Mutex;

pub(crate) fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) struct TestEnvGuard {
    previous: Vec<(String, Option<String>)>,
}

impl TestEnvGuard {
    pub(crate) fn set(vars: &[(&str, String)]) -> Self {
        let previous = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect::<Vec<_>>();
        for (key, value) in vars {
            unsafe { std::env::set_var(key, value) };
        }
        Self { previous }
    }

    pub(crate) fn remove(keys: &[&str]) -> Self {
        let previous = keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect::<Vec<_>>();
        for key in keys {
            unsafe { std::env::remove_var(key) };
        }
        Self { previous }
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}
