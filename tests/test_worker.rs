use std::process::Command;
use std::sync::Arc;

use reqwest::{redirect::Policy, Client};
use thermite::{task::BaseTask, worker::execute_task};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn redirect_response_is_not_a_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        assert_ne!(stream.read(&mut buffer).await.unwrap(), 0);
        stream
            .write_all(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });

    let client = Arc::new(
        Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .build()
            .unwrap(),
    );
    let task = BaseTask {
        task: format!("http://{address}/redirect"),
        ..Default::default()
    };

    let result = execute_task(client, task).await;
    server.await.unwrap();

    assert!(result.is_err(), "HTTP 302 must enter retry handling");
}

#[test]
fn receiver_rejects_outbound_auth_without_an_inbound_api_key() {
    let token = "test-only-outbound-secret";
    for api_key in [None, Some(""), Some("  ")] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_thermite"));
        command
            .env_clear()
            .env("THERMITE_OUTBOUND_BEARER_TOKEN", token)
            .env(
                "THERMITE_OUTBOUND_BEARER_URLS",
                "https://api.example.com/jobs/refresh/",
            )
            .args(["--mode", "receiver"]);
        if let Some(api_key) = api_key {
            command.env("THERMITE_API_KEY", api_key);
        }
        let output = command.output().unwrap();

        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("THERMITE_API_KEY"));
        assert!(!stderr.contains(token));
    }
}

#[test]
fn incomplete_outbound_auth_fails_before_startup() {
    let token = "test-only-outbound-secret";
    let output = Command::new(env!("CARGO_BIN_EXE_thermite"))
        .env_clear()
        .env("THERMITE_OUTBOUND_BEARER_TOKEN", token)
        .args(["--mode", "receiver"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("THERMITE_OUTBOUND_BEARER_URLS"));
    assert!(!stderr.contains(token));
}

#[test]
fn fetcher_does_not_require_the_receiver_api_key() {
    let token = "test-only-outbound-secret";
    let output = Command::new(env!("CARGO_BIN_EXE_thermite"))
        .env_clear()
        .env("THERMITE_OUTBOUND_BEARER_TOKEN", token)
        .env(
            "THERMITE_OUTBOUND_BEARER_URLS",
            "https://api.example.com/jobs/refresh/",
        )
        .args(["--mode", "fetcher"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("FETCH_URL must be set"));
    assert!(!stderr.contains("THERMITE_API_KEY"));
    assert!(!stderr.contains(token));
}

#[test]
fn receiver_accepts_an_api_key_before_connecting_to_redis() {
    let token = "test-only-outbound-secret";
    let api_key = "test-only-inbound-key";
    let output = Command::new(env!("CARGO_BIN_EXE_thermite"))
        .env_clear()
        .env("THERMITE_OUTBOUND_BEARER_TOKEN", token)
        .env(
            "THERMITE_OUTBOUND_BEARER_URLS",
            "https://api.example.com/jobs/refresh/",
        )
        .env("THERMITE_API_KEY", api_key)
        .args(["--mode", "receiver", "--redis-url", "invalid"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Invalid Redis URL"));
    assert!(!stderr.contains(token));
    assert!(!stderr.contains(api_key));
}
