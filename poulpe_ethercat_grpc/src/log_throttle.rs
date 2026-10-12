use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

/// Shared across streams so reconnects and multiple subscribers cannot multiply
/// the repeated feedback/command failure logs for a slave.
#[derive(Debug, Default)]
pub(crate) struct LogThrottle {
    last_logged: Mutex<HashMap<(u32, &'static str), Instant>>,
}

impl LogThrottle {
    pub(crate) fn should_log(&self, slave: u32, operation: &'static str) -> bool {
        self.should_log_at(slave, operation, Instant::now())
    }

    fn should_log_at(&self, slave: u32, operation: &'static str, now: Instant) -> bool {
        let mut last_logged = self.last_logged.lock().unwrap();
        let key = (slave, operation);
        if let Some(last) = last_logged.get(&key) {
            if now.duration_since(*last) < Duration::from_secs(5) {
                return false;
            }
        }
        last_logged.insert(key, now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_first_failure_and_limits_repeats_per_slave_and_operation() {
        let throttle = LogThrottle::default();
        let now = Instant::now();
        assert!(throttle.should_log_at(3, "feedback", now));
        for ms in 1..5000 {
            assert!(!throttle.should_log_at(3, "feedback", now + Duration::from_millis(ms)));
        }
        assert!(throttle.should_log_at(4, "feedback", now));
        assert!(throttle.should_log_at(3, "command", now));
        assert!(throttle.should_log_at(3, "feedback", now + Duration::from_secs(5)));
    }
}
