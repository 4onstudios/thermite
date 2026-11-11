use std::time::Duration;

use reqwest::StatusCode;
use thermite::queue::handle_task_failure;
use thermite::task::BaseTask;
use thermite::worker::TaskExecutionError;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::timeout;

async fn read_command<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Vec<String> {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let count = line
        .trim_end()
        .strip_prefix('*')
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let mut arguments = Vec::new();
    for _ in 0..count {
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let length = line
            .trim_end()
            .strip_prefix('$')
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut bytes = vec![0; length + 2];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes[length..], b"\r\n");
        arguments.push(String::from_utf8(bytes[..length].to_vec()).unwrap());
    }
    arguments
}

async fn capture_failure_write(task: &BaseTask, message: &str) -> Vec<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        for _ in 0..2 {
            let command = read_command(&mut reader).await;
            assert_eq!(command[0], "CLIENT");
            write.write_all(b"+OK\r\n").await.unwrap();
        }
        let command = read_command(&mut reader).await;
        write.write_all(b":1\r\n").await.unwrap();
        command
    });

    let client = redis::Client::open(format!("redis://{address}/")).unwrap();
    timeout(
        Duration::from_secs(3),
        handle_task_failure(&client, task, message),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap()
}

#[tokio::test]
async fn http_failures_are_retried_and_then_written_to_the_dead_letter_queue() {
    let error = TaskExecutionError::HttpStatus(StatusCode::FORBIDDEN).to_string();
    let task = BaseTask {
        id: "retry-task".to_string(),
        task: "https://api.example.com/job/".to_string(),
        max_retries: 1,
        ..Default::default()
    };

    let retry = capture_failure_write(&task, &error).await;
    assert_eq!(&retry[..2], ["ZADD", "task_queue"]);
    let retried_task: BaseTask = serde_json::from_str(retry.last().unwrap()).unwrap();
    assert_eq!(retried_task.retry_count, 1);
    assert!(retried_task.is_retry);
    assert_eq!(retried_task.last_error.as_deref(), Some(error.as_str()));

    let dead_letter = capture_failure_write(&retried_task, &error).await;
    assert_eq!(&dead_letter[..2], ["RPUSH", "dead_letter_queue"]);
    let dead_task: BaseTask = serde_json::from_str(dead_letter.last().unwrap()).unwrap();
    assert_eq!(dead_task.retry_count, 1);
    assert_eq!(dead_task.last_error.as_deref(), Some(error.as_str()));
}
