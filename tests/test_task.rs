#[cfg(test)]
mod tests {
    use chrono::Utc;
    use thermite::task::BaseTask;

    #[test]
    fn test_get_next_unix_datetime_non_periodic() {
        let task = BaseTask {
            category: "non_periodic".to_string(),
            scheduled_at: 1628764800, // Unix timestamp for August 13, 2021 00:00:00 UTC
            cron_scheduled_at: "0 0 * * *".to_string(), // Cron schedule for daily at midnight
            ..Default::default()
        };

        let next_datetime = task.get_next_unix_datetime().unwrap();
        assert_eq!(next_datetime, 1628764800); // Scheduled datetime should remain the same
    }

    #[test]
    fn test_get_next_unix_datetime_periodic() {
        let current = 1628764800_i64; // Unix timestamp for August 13, 2021 00:00:00 UTC
        let task = BaseTask {
            category: "periodic".to_string(),
            scheduled_at: current as u64, // Unix timestamp for August 13, 2021 00:00:00 UTC
            cron_scheduled_at: "0 0 * * *".to_string(), // Cron schedule for daily at midnight
            ..Default::default()
        };

        let next_datetime = task.get_next_unix_datetime().unwrap();
        // today at midnight

        assert!(next_datetime > Utc::now().timestamp()); // Unix timestamp for August 14, 2021 00:00:00 UTC
    }

    #[test]
    fn test_set_next_unix_datetime_non_periodic() {
        let mut task = BaseTask {
            category: "non_periodic".to_string(),
            scheduled_at: 1628764800, // Unix timestamp for August 13, 2021 00:00:00 UTC
            cron_scheduled_at: "0 0 * * *".to_string(), // Cron schedule for daily at midnight
            ..Default::default()
        };

        task.set_next_unix_datetime().unwrap();
        assert_eq!(task.scheduled_at, 1628764800); // Scheduled datetime should remain the same
    }

    #[test]
    fn test_set_next_unix_datetime_periodic() {
        let mut task = BaseTask {
            category: "periodic".to_string(),
            scheduled_at: 1628764800, // Unix timestamp for August 13, 2021 00:00:00 UTC
            cron_scheduled_at: "0 0 * * *".to_string(), // Cron schedule for daily at midnight
            ..Default::default()
        };

        task.set_next_unix_datetime().unwrap();
        assert!(task.scheduled_at as i64 > Utc::now().timestamp());
    }

    #[test]
    fn test_periodic_schedule_is_recalculated_for_each_run() {
        let mut task = BaseTask {
            category: "periodic".to_string(),
            scheduled_at: Utc::now().timestamp() as u64,
            cron_scheduled_at: "* * * * *".to_string(),
            ..Default::default()
        };

        task.set_next_unix_datetime().unwrap();
        let first_run = task.scheduled_at;

        task.set_next_unix_datetime().unwrap();
        let second_run = task.scheduled_at;

        assert!(second_run > first_run);
    }

    #[test]
    fn test_schedule_retry_increments_count_and_records_error() {
        std::env::set_var("THERMITE_RETRY_BASE_DELAY_SECS", "1");

        let mut task = BaseTask {
            id: "retry-task".to_string(),
            scheduled_at: Utc::now().timestamp() as u64,
            max_retries: 3,
            ..Default::default()
        };

        let should_retry = task.schedule_retry("temporary network failure");
        std::env::remove_var("THERMITE_RETRY_BASE_DELAY_SECS");

        assert!(should_retry);
        assert_eq!(task.retry_count, 1);
        assert_eq!(task.last_error.as_deref(), Some("temporary network failure"));
        assert!(task.scheduled_at > Utc::now().timestamp() as u64);
    }

    #[test]
    fn test_schedule_retry_stops_at_max_retries() {
        let mut task = BaseTask {
            id: "retry-task".to_string(),
            retry_count: 1,
            max_retries: 1,
            ..Default::default()
        };

        let should_retry = task.schedule_retry("permanent failure");

        assert!(!should_retry);
        assert_eq!(task.retry_count, 1);
        assert_eq!(task.last_error.as_deref(), Some("permanent failure"));
    }
}
