use crate::{log_throttle::LogThrottle, pb::PoulpeState};

/// Publish only successful reads from this cycle. Missing slaves are omitted;
/// their last measurements must never be replayed as new feedback.
pub(crate) fn collect_feedback(
    ids: &[i32],
    mut read: impl FnMut(i32) -> Result<PoulpeState, Box<dyn std::error::Error>>,
    failure_logs: &LogThrottle,
) -> Vec<PoulpeState> {
    ids.iter().filter_map(|&id| match read(id) {
        Ok(state) => Some(state),
        Err(e) => {
            if failure_logs.should_log(id as u32, "feedback") {
                log::error!("Failed to get state for slave {}: {}. Omitting feedback (repeated failures logged at most every 5s)", id, e);
            }
            None
        }
    }).collect()
}

#[cfg(test)]
mod feedback_tests {
    use super::*;
    use prost_types::Timestamp;

    fn sample(id: i32, seconds: i64) -> PoulpeState {
        PoulpeState {
            id,
            published_timestamp: Some(Timestamp { seconds, nanos: 0 }),
            ..Default::default()
        }
    }

    #[test]
    fn disconnect_omits_failed_slave_and_recovery_uses_new_sample() {
        let logs = LogThrottle::default();
        let ids = [4, 5];
        assert_eq!(
            collect_feedback(&ids, |id| Ok(sample(id, 100)), &logs),
            vec![sample(4, 100), sample(5, 100)]
        );
        for _ in 0..1000 {
            let states = collect_feedback(
                &ids,
                |id| {
                    if id == 5 {
                        Err("Slave not ready!".into())
                    } else {
                        Ok(sample(id, 101))
                    }
                },
                &logs,
            );
            assert_eq!(states, vec![sample(4, 101)]);
        }
        assert_eq!(
            collect_feedback(&ids, |id| Ok(sample(id, 200)), &logs),
            vec![sample(4, 200), sample(5, 200)]
        );
    }

    #[test]
    fn total_dropout_produces_no_feedback() {
        let logs = LogThrottle::default();
        assert!(collect_feedback(&[4, 5], |_| Err("Slave not ready!".into()), &logs).is_empty());
    }
}
