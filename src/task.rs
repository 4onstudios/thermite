use std::net::IpAddr;
use std::str::FromStr;

use chrono::{TimeZone, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use tracing::debug;
use url::Url;

use crate::errors::TaskQueueError;

fn default_max_retries() -> u32 {
    std::env::var("THERMITE_MAX_RETRIES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(3)
}

fn default_retry_base_delay_secs() -> u64 {
    std::env::var("THERMITE_RETRY_BASE_DELAY_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
}

/// A structure holding two public integers.
///
/// Example:
///
/// ```
/// use thermite::task::BaseTask;
///
/// let task = BaseTask {
///    id: "1".to_string(),
///    name: "Task 1".to_string(),
///    description: "Task 1 description".to_string(),
///    category: "non_periodic".to_string(),
///    priority: "high".to_string(),
///    task: "http://localhost:8080/task".to_string(),
///    scheduled_at: 1628764800,
///    cron_scheduled_at: "* 0 0 * * *".to_string(),
///    args: None,
///    max_retries: 3,
///    retry_count: 0,
///    last_error: None,
///    is_retry: false,
/// };
///
/// assert_eq!(task.id, "1");
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BaseTask {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub priority: String,
    pub task: String,
    pub scheduled_at: u64,
    pub cron_scheduled_at: String,
    pub args: Option<std::collections::HashMap<String, serde_json::Value>>,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub is_retry: bool,
}


// Define a struct to match the incoming JSON payload
#[derive(Serialize, Deserialize, Debug)]
pub struct BaseTaskPayload {
    pub id: String,
    pub name: String,
    pub description: String,
    pub category: String,
    pub priority: String,
    pub task: String,
    pub scheduled_at: u64,
    pub cron_scheduled_at: String,
    pub args: Option<std::collections::HashMap<String, serde_json::Value>>,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub is_retry: bool,
}

impl From<BaseTaskPayload> for BaseTask {
    fn from(payload: BaseTaskPayload) -> Self {
        BaseTask {
            id: payload.id,
            name: payload.name,
            description: payload.description,
            category: payload.category,
            priority: payload.priority,
            task: payload.task,
            scheduled_at: payload.scheduled_at,
            cron_scheduled_at: payload.cron_scheduled_at,
            args: payload.args,
            max_retries: payload.max_retries,
            retry_count: payload.retry_count,
            last_error: payload.last_error,
            is_retry: payload.is_retry,
        }
    }
}

impl Default for BaseTask {
    fn default() -> Self {
        BaseTask {
            id: "".to_string(),
            name: "".to_string(),
            description: "".to_string(),
            category: "".to_string(),
            priority: "".to_string(),
            task: "".to_string(),
            scheduled_at: 0,
            cron_scheduled_at: "".to_string(),
            args: None,
            max_retries: default_max_retries(),
            retry_count: 0,
            last_error: None,
            is_retry: false,
        }
    }
}


impl BaseTask {
    fn env_flag(name: &str) -> bool {
        std::env::var(name)
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    }

    fn is_disallowed_ip_address(ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(address) => {
                address.is_private()
                    || address.is_loopback()
                    || address.is_link_local()
                    || address.is_broadcast()
                    || address.is_documentation()
                    || address.is_unspecified()
                    || address.is_multicast()
            }
            IpAddr::V6(address) => {
                let first_segment = address.segments()[0];

                address.is_loopback()
                    || address.is_unspecified()
                    || (first_segment & 0xfe00) == 0xfc00
                    || (first_segment & 0xffc0) == 0xfe80
                    || address.is_multicast()
            }
        }
    }

    fn is_host_allowed(host: &str, allowed_hosts: &str) -> bool {
        allowed_hosts
            .split(',')
            .map(|entry| entry.trim().to_ascii_lowercase())
            .filter(|entry| !entry.is_empty())
            .any(|entry| host == entry || host.ends_with(&format!(".{entry}")))
    }

    pub fn validate_target_url(&self) -> Result<(), TaskQueueError> {
        let parsed_url = Url::parse(&self.task).map_err(|e| {
            TaskQueueError::InvalidTaskTarget(format!("Invalid task URL '{}': {e}", self.task))
        })?;

        match parsed_url.scheme() {
            "https" => {}
            "http" if !Self::env_flag("THERMITE_REQUIRE_HTTPS") => {}
            "http" => {
                return Err(TaskQueueError::InvalidTaskTarget(
                    "Only HTTPS task URLs are allowed when THERMITE_REQUIRE_HTTPS is enabled".to_string(),
                ));
            }
            scheme => {
                return Err(TaskQueueError::InvalidTaskTarget(format!(
                    "Unsupported task URL scheme: {scheme}"
                )));
            }
        }

        let host = parsed_url
            .host_str()
            .ok_or_else(|| TaskQueueError::InvalidTaskTarget("Task URL must include a host".to_string()))?
            .to_ascii_lowercase();

        if host == "localhost" || host.ends_with(".localhost") {
            return Err(TaskQueueError::InvalidTaskTarget(format!(
                "Task host '{host}' is not allowed"
            )));
        }

        if let Ok(ip_address) = host.parse::<IpAddr>() {
            if Self::is_disallowed_ip_address(ip_address) {
                return Err(TaskQueueError::InvalidTaskTarget(format!(
                    "Task host '{host}' resolves to a blocked IP range"
                )));
            }
        }

        if let Ok(allowed_hosts) = std::env::var("THERMITE_ALLOWED_HOSTS") {
            if !allowed_hosts.trim().is_empty() && !Self::is_host_allowed(&host, &allowed_hosts) {
                return Err(TaskQueueError::InvalidTaskTarget(format!(
                    "Task host '{host}' is not in THERMITE_ALLOWED_HOSTS"
                )));
            }
        }

        Ok(())
    }

    pub fn validate(&self) -> Result<(), TaskQueueError> {
        self.validate_target_url()?;

        if self.category == "periodic" {
            let _ = self.get_next_unix_datetime()?;
        }

        Ok(())
    }

    pub fn schedule_retry(&mut self, error_message: &str) -> bool {
        self.last_error = Some(error_message.to_string());

        if self.retry_count >= self.max_retries {
            return false;
        }

        self.retry_count = self.retry_count.saturating_add(1);
        self.is_retry = true;

        let retry_multiplier = 1_u64
            .checked_shl(self.retry_count.saturating_sub(1))
            .unwrap_or(u64::MAX);
        let retry_delay = default_retry_base_delay_secs().saturating_mul(retry_multiplier);
        self.scheduled_at = (Utc::now().timestamp().max(0) as u64).saturating_add(retry_delay);

        true
    }

    /// Returns the next occurrence of a Unix datetime based on the task's cron schedule.
    /// If the task's category is not "periodic", the method returns the scheduled datetime as an i64.
    /// If the task's category is "periodic", the method parses the cron schedule and calculates the next occurrence.
    /// The cron schedule is expected to have either 5 or 6 fields separated by spaces.
    /// If the cron schedule has 5 fields, the method prepends '0' to make it a 6-field cron string.
    /// The method then creates a `Schedule` instance from the cron string and retrieves the next occurrence using the `upcoming` method.
    /// If no upcoming dates are found, the method returns an error.
    /// The next occurrence is returned as a Unix datetime (i64).
    ///
    /// # Examples
    ///
    /// ```
    /// use thermite::task::BaseTask;
    ///
    /// let task = BaseTask {
    ///   id: "1".to_string(),
    ///   name: "Task 1".to_string(),
    ///   description: "Task 1 description".to_string(),
    ///   category: "periodic".to_string(),
    ///   priority: "high".to_string(),
    ///   task: "http://localhost:8080/task".to_string(),
    ///   scheduled_at: 1628764800,
    ///   cron_scheduled_at: "0 0 * * *".to_string(),
    ///   args: None,
    ///   max_retries: 3,
    ///   retry_count: 0,
    ///   last_error: None,
    ///   is_retry: false,
    /// };
    ///
    /// let next_datetime = task.get_next_unix_datetime().unwrap();
    /// assert!(next_datetime > 1628764800);
    ///
    /// ```
    pub fn get_next_unix_datetime(&self) -> Result<i64, TaskQueueError> {
        if self.category != "periodic" {
            return Ok(self.scheduled_at as i64);
        }

        let cron_expression = self.cron_scheduled_at.trim();
        let cron_schedule = if cron_expression.split_whitespace().count() == 5 {
            format!("0 {}", cron_expression)
        } else {
            cron_expression.to_owned()
        };

        debug!(cron_schedule = %cron_schedule, "normalized cron schedule for periodic task");

        let schedule = Schedule::from_str(&cron_schedule)
            .map_err(|e| TaskQueueError::InvalidCronExpression(e.to_string()))?;

        let now_ts = Utc::now().timestamp();
        let base_ts = std::cmp::max(now_ts, self.scheduled_at as i64);
        let base_datetime = Utc
            .timestamp_opt(base_ts, 0)
            .single()
            .ok_or_else(|| TaskQueueError::InvalidCronExpression("Invalid base timestamp for schedule".to_string()))?;

        let next_occurrence = schedule
            .after(&base_datetime)
            .next()
            .ok_or_else(|| TaskQueueError::InvalidCronExpression("No upcoming dates found".to_string()))?;

        debug!(
            base_ts,
            next_occurrence = %next_occurrence,
            "computed next periodic execution time"
        );

        Ok(next_occurrence.timestamp())
    }


    /// Set the next scheduled Unix datetime based on the task's cron schedule.
    /// If the task's category is not "periodic", the method sets the scheduled_at value to the current value.
    /// If the task's category is "periodic", the method calculates the next occurrence based on the cron schedule and sets the scheduled_at value to it.
    /// The method internally calls the `get_next_unix_datetime` method to calculate the next occurrence.
    ///
    /// # Examples
    ///
    /// ```
    /// use thermite::task::BaseTask;
    ///
    /// let mut task = BaseTask {
    ///     id: "1".to_string(),
    ///     name: "Task 1".to_string(),
    ///     description: "Task 1 description".to_string(),
    ///     category: "periodic".to_string(),
    ///     priority: "high".to_string(),
    ///     task: "http://localhost:8080/task".to_string(),
    ///     scheduled_at: 1628764800,
    ///     cron_scheduled_at: "* 0 0 * * *".to_string(),
    ///     args: None,
    ///     max_retries: 3,
    ///     retry_count: 0,
    ///     last_error: None,
    ///     is_retry: false,
    /// };
    ///
    /// task.set_next_unix_datetime().unwrap();
    /// assert!(task.scheduled_at > 1628764800);
    /// ```
    pub fn set_next_unix_datetime(&mut self) -> Result<(), TaskQueueError> {
        self.scheduled_at = self.get_next_unix_datetime()? as u64;
        Ok(())
    }
}
