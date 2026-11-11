use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use native_tls::{Identity, TlsAcceptor};
use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::hash::MessageDigest;
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::rsa::Rsa;
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509NameBuilder, X509};
use reqwest::{Certificate, Client, ClientBuilder, StatusCode};
use thermite::task::BaseTask;
use thermite::worker::{OutboundBearerAuth, TaskExecutionError, TaskExecutor};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

const TEST_HOST: &str = "thermite-test.example";
const TEST_TOKEN: &str = "test-only-outbound-secret";
const TEST_IDENTITY_PASSWORD: &str = "test-only-identity-password";
const OK_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const FORBIDDEN_RESPONSE: &[u8] =
    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const INVALID_RESPONSE: &[u8] = b"not-an-http-response\r\n\r\n";

fn test_tls() -> (tokio_native_tls::TlsAcceptor, Certificate) {
    let root_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut root_name = X509NameBuilder::new().unwrap();
    root_name
        .append_entry_by_text("CN", "Thermite test CA")
        .unwrap();
    let root_name = root_name.build();
    let mut root = X509::builder().unwrap();
    root.set_version(2).unwrap();
    let root_serial = BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
    root.set_serial_number(&root_serial).unwrap();
    root.set_subject_name(&root_name).unwrap();
    root.set_issuer_name(&root_name).unwrap();
    root.set_pubkey(&root_key).unwrap();
    root.set_not_before(Asn1Time::days_from_now(0).unwrap().as_ref())
        .unwrap();
    root.set_not_after(Asn1Time::days_from_now(1).unwrap().as_ref())
        .unwrap();
    root.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
        .unwrap();
    root.append_extension(KeyUsage::new().key_cert_sign().crl_sign().build().unwrap())
        .unwrap();
    root.sign(&root_key, MessageDigest::sha256()).unwrap();
    let root = root.build();

    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_text("CN", TEST_HOST).unwrap();
    let name = name.build();
    let mut certificate = X509::builder().unwrap();
    certificate.set_version(2).unwrap();
    let serial = BigNum::from_u32(2).unwrap().to_asn1_integer().unwrap();
    certificate.set_serial_number(&serial).unwrap();
    certificate.set_subject_name(&name).unwrap();
    certificate.set_issuer_name(root.subject_name()).unwrap();
    certificate.set_pubkey(&key).unwrap();
    certificate
        .set_not_before(Asn1Time::days_from_now(0).unwrap().as_ref())
        .unwrap();
    certificate
        .set_not_after(Asn1Time::days_from_now(1).unwrap().as_ref())
        .unwrap();
    certificate
        .append_extension(BasicConstraints::new().critical().build().unwrap())
        .unwrap();
    certificate
        .append_extension(
            KeyUsage::new()
                .digital_signature()
                .key_encipherment()
                .build()
                .unwrap(),
        )
        .unwrap();
    certificate
        .append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap())
        .unwrap();
    let names = SubjectAlternativeName::new()
        .dns(TEST_HOST)
        .build(&certificate.x509v3_context(None, None))
        .unwrap();
    certificate.append_extension(names).unwrap();
    certificate
        .sign(&root_key, MessageDigest::sha256())
        .unwrap();
    let certificate = certificate.build();

    let mut archive = Pkcs12::builder();
    archive.name("thermite-test").pkey(&key).cert(&certificate);
    let archive = archive
        .build2(TEST_IDENTITY_PASSWORD)
        .unwrap()
        .to_der()
        .unwrap();
    let identity = Identity::from_pkcs12(&archive, TEST_IDENTITY_PASSWORD).unwrap();
    let acceptor = tokio_native_tls::TlsAcceptor::from(TlsAcceptor::new(identity).unwrap());
    let trusted_certificate = Certificate::from_pem(&root.to_pem().unwrap()).unwrap();
    (acceptor, trusted_certificate)
}

fn client_builder(address: SocketAddr, certificate: &Certificate) -> ClientBuilder {
    Client::builder()
        .no_proxy()
        .resolve(TEST_HOST, address)
        .add_root_certificate(certificate.clone())
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> String {
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

fn task(target: &str) -> BaseTask {
    BaseTask {
        id: "tls-test-task".to_string(),
        name: "TLS test task".to_string(),
        task: target.to_string(),
        max_retries: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn configured_https_auth_is_scoped_and_never_follows_redirects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (acceptor, certificate) = test_tls();
    let base = format!("https://{TEST_HOST}:{}", address.port());
    let allowed = format!("{base}/allowed/");
    let other = format!("{base}/other/");
    let redirect = format!(
        "HTTP/1.1 302 Found\r\nLocation: {base}/redirect-target/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in [
            OK_RESPONSE,
            OK_RESPONSE,
            FORBIDDEN_RESPONSE,
            redirect.as_bytes(),
            INVALID_RESPONSE,
        ] {
            let (stream, _) = timeout(Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            requests.push(read_request(&mut stream).await);
            stream.write_all(response).await.unwrap();
        }
        let redirected = timeout(Duration::from_millis(500), listener.accept())
            .await
            .is_ok();
        (requests, redirected)
    });

    let auth = OutboundBearerAuth::from_values(Some(TEST_TOKEN), Some(&allowed))
        .unwrap()
        .unwrap();
    let plain_client = Arc::new(client_builder(address, &certificate).build().unwrap());
    let executor = TaskExecutor::new(
        plain_client,
        client_builder(address, &certificate).redirect(reqwest::redirect::Policy::limited(10)),
        Some(auth),
    )
    .unwrap();

    let allowed_task = task(&allowed);
    assert!(!serde_json::to_string(&allowed_task)
        .unwrap()
        .contains(TEST_TOKEN));
    executor.execute_task(allowed_task).await.unwrap();
    executor.execute_task(task(&other)).await.unwrap();
    let mut failing_task = task(&allowed);
    let error = executor
        .execute_task(failing_task.clone())
        .await
        .err()
        .unwrap();
    assert!(matches!(
        &error,
        TaskExecutionError::HttpStatus(status) if *status == StatusCode::FORBIDDEN
    ));
    assert!(failing_task.schedule_retry(&error.to_string()));
    assert_eq!(failing_task.retry_count, 1);
    assert!(!format!("{error}").contains(TEST_TOKEN));

    let error = executor.execute_task(task(&allowed)).await.err().unwrap();
    assert!(matches!(
        &error,
        TaskExecutionError::HttpStatus(status) if *status == StatusCode::FOUND
    ));
    assert!(!format!("{error}").contains(TEST_TOKEN));

    let error = executor.execute_task(task(&allowed)).await.err().unwrap();
    assert!(matches!(&error, TaskExecutionError::Request(_)));
    assert!(!format!("{error}").contains(TEST_TOKEN));
    assert!(!format!("{error:?}").contains(TEST_TOKEN));

    let (requests, redirected) = server.await.unwrap();
    let expected = format!("Bearer {TEST_TOKEN}");
    assert_eq!(request_authorization(&requests[0]), Some(expected.as_str()));
    assert_eq!(request_authorization(&requests[1]), None);
    assert_eq!(request_authorization(&requests[2]), Some(expected.as_str()));
    assert_eq!(request_authorization(&requests[3]), Some(expected.as_str()));
    assert_eq!(request_authorization(&requests[4]), Some(expected.as_str()));
    assert!(!redirected);
    for request in requests {
        assert!(!request
            .split_once("\r\n\r\n")
            .unwrap()
            .1
            .contains(TEST_TOKEN));
    }
}
