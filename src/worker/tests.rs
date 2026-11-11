use std::time::Duration;

use reqwest::redirect::Policy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use super::*;

const TEST_TOKEN: &str = "test-only-outbound-secret";
const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

fn configured_auth(urls: &str) -> OutboundBearerAuth {
    OutboundBearerAuth::from_values(Some(TEST_TOKEN), Some(urls))
        .unwrap()
        .unwrap()
}

fn test_executor(auth: Option<OutboundBearerAuth>) -> TaskExecutor {
    let client = Arc::new(Client::builder().no_proxy().build().unwrap());
    TaskExecutor::new(
        client,
        Client::builder().no_proxy().redirect(Policy::limited(10)),
        auth,
    )
    .unwrap()
}

fn task(target: &str) -> BaseTask {
    BaseTask {
        id: "test-task".to_string(),
        name: "Test task".to_string(),
        task: target.to_string(),
        ..Default::default()
    }
}

async fn read_request(stream: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let count = timeout(Duration::from_secs(3), stream.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            count, 0,
            "connection closed before the request was complete"
        );
        request.extend_from_slice(&buffer[..count]);
        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }
    String::from_utf8(request).unwrap()
}

fn request_authorization(request: &str) -> Option<&str> {
    request
        .split("\r\n\r\n")
        .next()?
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim())
}

#[test]
fn auth_configuration_is_optional_but_must_be_complete() {
    assert!(OutboundBearerAuth::from_values(None, None)
        .unwrap()
        .is_none());
    assert_eq!(
        OutboundBearerAuth::from_values(Some(TEST_TOKEN), None).err(),
        Some(OutboundAuthConfigError::MissingSetting)
    );
    assert_eq!(
        OutboundBearerAuth::from_values(None, Some("https://api.example.com/job/")).err(),
        Some(OutboundAuthConfigError::MissingSetting)
    );

    for token in ["", " ", "two words", "bad\r\ntoken", "non-ascii-\u{e9}"] {
        assert_eq!(
            OutboundBearerAuth::from_values(Some(token), Some("https://api.example.com/job/"))
                .err(),
            Some(OutboundAuthConfigError::InvalidToken)
        );
    }
}

#[test]
fn auth_configuration_rejects_unsafe_urls_without_exposing_credentials() {
    for url in [
        "",
        "http://api.example.com/job/",
        "https://user:password@api.example.com/job/",
        "https://api.example.com/job/?key=value",
        "https://api.example.com/job/#fragment",
        "https://api.example.com/job/,",
        "https://api.example.com/job/\nother",
        "not-a-url",
    ] {
        let error = OutboundBearerAuth::from_values(Some(TEST_TOKEN), Some(url))
            .err()
            .unwrap();
        assert!(matches!(error, OutboundAuthConfigError::InvalidUrl(_)));
        assert!(!format!("{error}").contains(TEST_TOKEN));
        assert!(!format!("{error}").contains("password"));
    }
}

#[test]
fn credentials_are_sensitive_and_bound_to_exact_urls() {
    let auth = configured_auth(
        "https://api.example.com/jobs/refresh/, https://api.example.com/jobs/retry/",
    );

    assert!(auth
        .authorization_for("https://api.example.com/jobs/refresh/")
        .is_some());
    assert!(auth
        .authorization_for("HTTPS://API.EXAMPLE.COM:443/jobs/retry/")
        .is_some());
    for target in [
        "http://api.example.com/jobs/refresh/",
        "https://other.example.com/jobs/refresh/",
        "https://api.example.com.evil.test/jobs/refresh/",
        "https://api.example.com:444/jobs/refresh/",
        "https://api.example.com/jobs/refresh",
        "https://api.example.com/jobs/refresh/other",
        "https://api.example.com/jobs/refresh/?other=1",
        "https://api.example.com/jobs/refresh/#other",
        "https://user@api.example.com/jobs/refresh/",
        "not-a-url",
    ] {
        assert!(auth.authorization_for(target).is_none(), "{target}");
    }
    assert!(auth.authorization.is_sensitive());
    assert!(!format!("{:?}", auth.authorization).contains(TEST_TOKEN));
}

#[tokio::test]
async fn unmatched_tasks_keep_their_existing_redirect_behavior() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let redirect_target = format!("{base}/redirect/");
    let redirect = format!(
        "HTTP/1.1 302 Found\r\nLocation: {base}/destination/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in [redirect.as_bytes(), OK_RESPONSE] {
            let (mut stream, _) = timeout(Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
            requests.push(read_request(&mut stream).await);
            stream.write_all(response).await.unwrap();
        }
        requests
    });

    let executor = test_executor(Some(configured_auth("https://api.example.com/allowed/")));
    assert!(executor.execute_task(task(&redirect_target)).await.is_ok());
    for request in server.await.unwrap() {
        assert!(request_authorization(&request).is_none());
    }
}
