use std::collections::HashSet;
use std::env;
use std::sync::Arc;

use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::redirect::Policy;
use reqwest::{Client, ClientBuilder, Response, StatusCode};
use serde_json::json;
use thiserror::Error;
use tracing::{error, info};
use url::Url;

use crate::task::BaseTask;

const TOKEN_ENV: &str = "THERMITE_OUTBOUND_BEARER_TOKEN";
const URLS_ENV: &str = "THERMITE_OUTBOUND_BEARER_URLS";

#[derive(Debug, Error)]
pub enum TaskExecutionError {
    #[error("task request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("task endpoint returned HTTP {0}")]
    HttpStatus(StatusCode),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OutboundAuthConfigError {
    #[error("THERMITE_OUTBOUND_BEARER_TOKEN and THERMITE_OUTBOUND_BEARER_URLS must both be set")]
    MissingSetting,
    #[error("THERMITE_OUTBOUND_BEARER_TOKEN must be non-empty ASCII without whitespace")]
    InvalidToken,
    #[error(
        "THERMITE_OUTBOUND_BEARER_URLS entry {0} must be an HTTPS URL without user info, query or fragment"
    )]
    InvalidUrl(usize),
    #[error("{0} must contain valid Unicode")]
    InvalidEnvironment(&'static str),
}

pub struct OutboundBearerAuth {
    authorization: HeaderValue,
    urls: HashSet<Url>,
}

impl OutboundBearerAuth {
    pub fn from_env() -> Result<Option<Self>, OutboundAuthConfigError> {
        fn read(name: &'static str) -> Result<Option<String>, OutboundAuthConfigError> {
            match env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(env::VarError::NotUnicode(_)) => {
                    Err(OutboundAuthConfigError::InvalidEnvironment(name))
                }
            }
        }

        let token = read(TOKEN_ENV)?;
        let urls = read(URLS_ENV)?;
        Self::from_values(token.as_deref(), urls.as_deref())
    }

    pub fn from_values(
        token: Option<&str>,
        urls: Option<&str>,
    ) -> Result<Option<Self>, OutboundAuthConfigError> {
        let (token, urls) = match (token, urls) {
            (None, None) => return Ok(None),
            (Some(token), Some(urls)) => (token, urls),
            _ => return Err(OutboundAuthConfigError::MissingSetting),
        };

        if token.is_empty()
            || !token.is_ascii()
            || token
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(OutboundAuthConfigError::InvalidToken);
        }

        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| OutboundAuthConfigError::InvalidToken)?;
        authorization.set_sensitive(true);

        let mut allowed_urls = HashSet::new();
        for (index, value) in urls.split(',').enumerate() {
            let value = value.trim();
            if value
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
            {
                return Err(OutboundAuthConfigError::InvalidUrl(index + 1));
            }
            let url =
                Url::parse(value).map_err(|_| OutboundAuthConfigError::InvalidUrl(index + 1))?;
            if url.scheme() != "https"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(OutboundAuthConfigError::InvalidUrl(index + 1));
            }
            allowed_urls.insert(url);
        }

        Ok(Some(Self {
            authorization,
            urls: allowed_urls,
        }))
    }

    fn authorization_for(&self, target: &str) -> Option<&HeaderValue> {
        let url = Url::parse(target).ok()?;
        self.urls.contains(&url).then_some(&self.authorization)
    }
}

struct AuthenticatedClient {
    client: Arc<Client>,
    auth: OutboundBearerAuth,
}

pub struct TaskExecutor {
    client: Arc<Client>,
    authenticated: Option<AuthenticatedClient>,
}

impl TaskExecutor {
    pub fn new(
        client: Arc<Client>,
        authenticated_builder: ClientBuilder,
        auth: Option<OutboundBearerAuth>,
    ) -> Result<Self, reqwest::Error> {
        let authenticated = match auth {
            Some(auth) => Some(AuthenticatedClient {
                client: Arc::new(authenticated_builder.redirect(Policy::none()).build()?),
                auth,
            }),
            None => None,
        };
        Ok(Self {
            client,
            authenticated,
        })
    }

    pub async fn execute_task(&self, task: BaseTask) -> Result<Response, TaskExecutionError> {
        if let Some(authenticated) = &self.authenticated {
            if let Some(authorization) = authenticated.auth.authorization_for(&task.task) {
                return execute_request(
                    Arc::clone(&authenticated.client),
                    task,
                    Some(authorization.clone()),
                )
                .await;
            }
        }
        execute_request(Arc::clone(&self.client), task, None).await
    }
}

pub async fn execute_task(
    client: Arc<Client>,
    task: BaseTask,
) -> Result<Response, TaskExecutionError> {
    execute_request(client, task, None).await
}

async fn execute_request(
    client: Arc<Client>,
    task: BaseTask,
    authorization: Option<HeaderValue>,
) -> Result<Response, TaskExecutionError> {
    let task_id = task.id.clone();
    let task_name = task.name.clone();

    info!(task_id = %task_id, task_name = %task_name, "executing task");

    let mut request = client
        .post(&task.task)
        .json(&json!({ "task_id": task.id, "args": task.args }));
    if let Some(authorization) = authorization {
        request = request.header(AUTHORIZATION, authorization);
    }
    let response = request.send().await?;
    let status = response.status();

    if status.is_success() {
        info!(task_id = %task_id, status = status.as_u16(), "task execution completed successfully");
        Ok(response)
    } else {
        error!(task_id = %task_id, status = status.as_u16(), "task execution returned a non-success status");
        Err(TaskExecutionError::HttpStatus(status))
    }
}

#[cfg(test)]
mod tests;
