//! Minimal HTTP/1.1 client for the Firecracker API served over a Unix socket.
//!
//! Firecracker's `micro_http` server answers `PUT` requests with
//! `204 No Content` on success and `400 Bad Request` +
//! `{"fault_message": "..."}` on error. Every request uses a fresh connection;
//! the response is delimited by `Content-Length` (never by EOF, since the
//! server keeps connections alive).

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("malformed HTTP response: {0}")]
    Malformed(String),
    #[error("{method} {path} -> {status} {reason}: {body}")]
    Status {
        method: String,
        path: String,
        status: u16,
        reason: String,
        body: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Format a request with a JSON body.
pub fn format_request(method: &str, path: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Accept: application/json\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

/// Parse an HTTP/1.1 response. The reader may deliver bytes in arbitrary chunks.
pub async fn read_response<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
) -> Result<HttpResponse, ApiError> {
    const MAX_HEAD: usize = 64 * 1024;
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    let head_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > MAX_HEAD {
            return Err(ApiError::Malformed("response head too large".into()));
        }
        let mut chunk = [0u8; 1024];
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            return Err(ApiError::Malformed(format!(
                "connection closed before end of headers ({} bytes read)",
                buf.len()
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = std::str::from_utf8(&buf[..head_end])
        .map_err(|e| ApiError::Malformed(format!("non-UTF-8 head: {e}")))?
        .to_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| ApiError::Malformed("empty status line".into()))?;
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(ApiError::Malformed(format!(
            "bad status line: {status_line:?}"
        )));
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ApiError::Malformed(format!("bad status line: {status_line:?}")))?;
    let reason = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once(':') else {
            return Err(ApiError::Malformed(format!("bad header line: {line:?}")));
        };
        headers.push((k.trim().to_owned(), v.trim().to_owned()));
    }
    let content_length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map(|(_, v)| {
            v.parse::<usize>()
                .map_err(|_| ApiError::Malformed(format!("bad Content-Length: {v:?}")))
        })
        .transpose()?
        .unwrap_or(0);
    let mut body = buf.split_off(head_end + 4);
    if body.len() < content_length {
        let missing = content_length - body.len();
        let mut rest = vec![0u8; missing];
        r.read_exact(&mut rest).await.map_err(|e| {
            ApiError::Malformed(format!(
                "connection closed inside body (wanted {content_length} bytes): {e}"
            ))
        })?;
        body.extend_from_slice(&rest);
    } else {
        body.truncate(content_length);
    }
    Ok(HttpResponse {
        status,
        reason,
        headers,
        body,
    })
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Client bound to one API socket.
#[derive(Debug, Clone)]
pub struct ApiClient {
    socket: PathBuf,
    timeout: Duration,
}

impl ApiClient {
    pub fn new(socket: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            socket: socket.into(),
            timeout,
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// `PUT <path>` with a JSON body; success means `204 No Content`
    /// (any 2xx is accepted).
    pub async fn put(&self, path: &str, body: &serde_json::Value) -> Result<(), ApiError> {
        self.send("PUT", path, body).await
    }

    /// `PATCH <path>` with a JSON body. Firecracker uses `PATCH` for the
    /// state changes of an already configured microVM — `PATCH /vm` with
    /// `{"state": "Paused"}` / `{"state": "Resumed"}` (docs/protocol.md §C) —
    /// and answers them exactly like `PUT`: `204 No Content` on success,
    /// `400 Bad Request` with `{"fault_message": "..."}` otherwise.
    pub async fn patch(&self, path: &str, body: &serde_json::Value) -> Result<(), ApiError> {
        self.send("PATCH", path, body).await
    }

    /// One request whose only interesting outcome is success; a non-2xx
    /// answer becomes [`ApiError::Status`] carrying the fault message.
    async fn send(
        &self,
        method: &str,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(), ApiError> {
        let resp = self.request(method, path, body).await?;
        if (200..300).contains(&resp.status) {
            Ok(())
        } else {
            let body = String::from_utf8_lossy(&resp.body).into_owned();
            Err(ApiError::Status {
                method: method.into(),
                path: path.into(),
                status: resp.status,
                reason: resp.reason,
                body: bounded(&body, 2048),
            })
        }
    }

    /// `GET <path>` without a body, answered with a JSON document (for
    /// example `GET /vm/config`, the configuration the VMM will boot with).
    pub async fn get_json(&self, path: &str) -> Result<serde_json::Value, ApiError> {
        let resp = self.request_bytes("GET", path, &[]).await?;
        if !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body).into_owned();
            return Err(ApiError::Status {
                method: "GET".into(),
                path: path.into(),
                status: resp.status,
                reason: resp.reason,
                body: bounded(&body, 2048),
            });
        }
        serde_json::from_slice(&resp.body)
            .map_err(|e| ApiError::Malformed(format!("GET {path}: body is not JSON: {e}")))
    }

    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<HttpResponse, ApiError> {
        let body = serde_json::to_vec(body).map_err(|e| ApiError::Malformed(e.to_string()))?;
        self.request_bytes(method, path, &body).await
    }

    async fn request_bytes(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<HttpResponse, ApiError> {
        let req = format_request(method, path, body);
        let fut = async {
            let mut stream = UnixStream::connect(&self.socket).await?;
            stream.write_all(&req).await?;
            stream.flush().await?;
            read_response(&mut stream).await
        };
        tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| ApiError::Timeout(self.timeout))?
    }
}

fn bounded(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// One-shot fake Firecracker API server: records the raw request and
    /// answers with the given response bytes.
    async fn fake_server(
        sock: PathBuf,
        response: &'static [u8],
    ) -> tokio::sync::oneshot::Receiver<Vec<u8>> {
        let listener = UnixListener::bind(&sock).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut req = Vec::new();
            // Read until the JSON body is complete (headers + Content-Length).
            loop {
                let mut chunk = [0u8; 256];
                let n = s.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                req.extend_from_slice(&chunk[..n]);
                if let Some(pos) = find_subslice(&req, b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&req[..pos]).into_owned();
                    let cl: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    if req.len() >= pos + 4 + cl {
                        break;
                    }
                }
            }
            // Deliver the response in two chunks to exercise partial reads.
            let (a, b) = response.split_at(response.len() / 2);
            s.write_all(a).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            s.write_all(b).await.unwrap();
            s.flush().await.unwrap();
            // Keep-alive: do not close; the client must rely on Content-Length.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(req);
        });
        rx
    }

    #[tokio::test]
    async fn put_formats_request_and_accepts_204() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("fc.sock");
        let rx = fake_server(
            sock.clone(),
            b"HTTP/1.1 204 No Content\r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n",
        )
        .await;
        let client = ApiClient::new(&sock, Duration::from_secs(2));
        client
            .put(
                "/machine-config",
                &serde_json::json!({"vcpu_count": 1, "mem_size_mib": 256, "smt": false}),
            )
            .await
            .unwrap();
        let req = String::from_utf8(rx.await.unwrap()).unwrap();
        let (head, body) = req.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("PUT /machine-config HTTP/1.1\r\n"));
        assert!(head.contains("Content-Type: application/json"));
        assert!(head.contains(&format!("Content-Length: {}", body.len())));
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["vcpu_count"], 1);
        assert_eq!(v["mem_size_mib"], 256);
        assert_eq!(v["smt"], false);
    }

    #[tokio::test]
    async fn put_surfaces_400_with_fault_message() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("fc.sock");
        let body = r#"{"fault_message":"The kernel file cannot be opened: No such file"}"#;
        let resp: &'static [u8] = Box::leak(
            format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            )
            .into_bytes()
            .into_boxed_slice(),
        );
        let _rx = fake_server(sock.clone(), resp).await;
        let client = ApiClient::new(&sock, Duration::from_secs(2));
        let err = client
            .put(
                "/boot-source",
                &serde_json::json!({"kernel_image_path": "/nope"}),
            )
            .await
            .unwrap_err();
        match err {
            ApiError::Status {
                status, path, body, ..
            } => {
                assert_eq!(status, 400);
                assert_eq!(path, "/boot-source");
                assert!(body.contains("kernel file cannot be opened"));
            }
            other => panic!("unexpected: {other}"),
        }
    }

    /// PLT-4633: idle quiesce / resume go out as `PATCH /vm`, and the body is
    /// exactly `{"state": "Paused"}` / `{"state": "Resumed"}`.
    #[tokio::test]
    async fn patch_sends_the_method_and_body_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("fc.sock");
        let rx = fake_server(
            sock.clone(),
            b"HTTP/1.1 204 No Content\r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n",
        )
        .await;
        let client = ApiClient::new(&sock, Duration::from_secs(2));
        client
            .patch("/vm", &serde_json::json!({"state": "Paused"}))
            .await
            .unwrap();
        let req = String::from_utf8(rx.await.unwrap()).unwrap();
        let (head, body) = req.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("PATCH /vm HTTP/1.1\r\n"), "{head}");
        assert_eq!(body, r#"{"state":"Paused"}"#);
    }

    #[tokio::test]
    async fn patch_surfaces_a_fault_message_with_its_method() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("fc.sock");
        let body = r#"{"fault_message":"The requested operation is not supported: Paused"}"#;
        let resp: &'static [u8] = Box::leak(
            format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            )
            .into_bytes()
            .into_boxed_slice(),
        );
        let _rx = fake_server(sock.clone(), resp).await;
        let client = ApiClient::new(&sock, Duration::from_secs(2));
        let err = client
            .patch("/vm", &serde_json::json!({"state": "Paused"}))
            .await
            .unwrap_err();
        match err {
            ApiError::Status {
                method,
                path,
                status,
                body,
                ..
            } => {
                assert_eq!(method, "PATCH");
                assert_eq!(path, "/vm");
                assert_eq!(status, 400);
                assert!(body.contains("not supported"), "{body}");
            }
            other => panic!("unexpected: {other}"),
        }
    }

    #[tokio::test]
    async fn read_response_parses_headers_and_body_from_chunks() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: b\r\n\r\nhelloEXTRA";
        let mut cursor = std::io::Cursor::new(raw.to_vec());
        let r = read_response(&mut cursor).await.unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.reason, "OK");
        assert_eq!(r.header("x-a"), Some("b"));
        assert_eq!(r.body, b"hello");
    }

    #[tokio::test]
    async fn read_response_rejects_garbage() {
        let mut cursor = std::io::Cursor::new(b"nonsense\r\n\r\n".to_vec());
        assert!(matches!(
            read_response(&mut cursor).await,
            Err(ApiError::Malformed(_))
        ));
        let mut cursor = std::io::Cursor::new(b"HTTP/1.1 204".to_vec());
        assert!(matches!(
            read_response(&mut cursor).await,
            Err(ApiError::Malformed(_))
        ));
    }

    #[tokio::test]
    async fn connect_failure_is_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let client = ApiClient::new(dir.path().join("missing.sock"), Duration::from_secs(1));
        assert!(matches!(
            client.put("/x", &serde_json::json!({})).await,
            Err(ApiError::Io(_))
        ));
    }
}
