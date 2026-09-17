//! Minimal HTTP/1.1 client for the in-guest Runtime API.
//!
//! The Runtime API lives on the loopback interface of the guest and is served
//! by the runtime bridge. A hand-written client keeps static musl guest
//! binaries small: no TLS, no connection pooling, one connection per request
//! (`Connection: close`). Responses are parsed by `Content-Length`, chunked
//! transfer encoding, or read-until-close, so the client does not depend on
//! how the server frames its bodies.
//!
//! Two I/O flavours share the same request builder and response parser: an
//! async one on `tokio::net::TcpStream` (used by the invocation loop; the
//! long-poll `GET /next` has no timeout) and a blocking one on
//! `std::net::TcpStream` (used by [`crate::init_error`], which must work even
//! when called before or outside a running runtime).

use std::io::{Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::SdkError;

/// Environment variable naming the Runtime API base URL.
pub const RUNTIME_API_ENV: &str = tachyon_serverless_protocol::env::RUNTIME_API;

/// Parsed HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    /// Header names are lower-cased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// First value of a header (name compared case-insensitively).
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Client bound to one Runtime API base URL.
#[derive(Debug, Clone)]
pub struct RuntimeClient {
    addr: SocketAddr,
    host_header: String,
}

impl RuntimeClient {
    /// Build a client from `TACHYON_RUNTIME_API`.
    pub fn from_env() -> Result<Self, SdkError> {
        let url =
            std::env::var(RUNTIME_API_ENV).map_err(|_| SdkError::MissingEnv(RUNTIME_API_ENV))?;
        Self::new(&url)
    }

    /// Build a client for a base URL such as `http://127.0.0.1:9001`.
    pub fn new(base_url: &str) -> Result<Self, SdkError> {
        let rest = base_url.strip_prefix("http://").ok_or_else(|| {
            SdkError::Protocol(format!(
                "runtime api url must start with http://: {base_url}"
            ))
        })?;
        let host_port = rest.split('/').next().unwrap_or("");
        if host_port.is_empty() {
            return Err(SdkError::Protocol(format!(
                "runtime api url has no host: {base_url}"
            )));
        }
        let addr = host_port
            .to_socket_addrs()
            .map_err(|e| {
                SdkError::Protocol(format!(
                    "cannot resolve runtime api address {host_port}: {e}"
                ))
            })?
            .next()
            .ok_or_else(|| {
                SdkError::Protocol(format!("cannot resolve runtime api address {host_port}"))
            })?;
        Ok(Self {
            addr,
            host_header: host_port.to_string(),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `GET path`. No timeout: the long-poll for the next event may take
    /// arbitrarily long.
    pub async fn get(&self, path: &str) -> Result<HttpResponse, SdkError> {
        self.request("GET", path, None).await
    }

    /// `POST path` with a JSON body.
    pub async fn post_json(&self, path: &str, body: &[u8]) -> Result<HttpResponse, SdkError> {
        self.request("POST", path, Some(body)).await
    }

    /// Perform one request over a fresh connection.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse, SdkError> {
        let mut stream = tokio::net::TcpStream::connect(self.addr).await?;
        stream.set_nodelay(true).ok();
        let request = build_request(method, path, &self.host_header, body);
        stream.write_all(&request).await?;
        let mut parser = ResponseParser::default();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return parser.finish_eof();
            }
            if let Some(resp) = parser.push(&chunk[..n])? {
                return Ok(resp);
            }
        }
    }

    /// Blocking variant of [`Self::request`] on `std::net`, with a timeout.
    /// Used where no async runtime is guaranteed (init errors).
    pub fn request_blocking(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        timeout: Duration,
    ) -> Result<HttpResponse, SdkError> {
        self.request_blocking_with(method, path, body, timeout, Some(timeout))
    }

    /// Blocking request whose response may take arbitrarily long when
    /// `read_timeout` is `None` (the experimental lifecycle's `continue`,
    /// which waits for a checkpoint/restore before any async runtime exists).
    pub fn request_blocking_with(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> Result<HttpResponse, SdkError> {
        let mut stream = std::net::TcpStream::connect_timeout(&self.addr, connect_timeout)?;
        stream.set_read_timeout(read_timeout)?;
        stream.set_write_timeout(Some(connect_timeout))?;
        let request = build_request(method, path, &self.host_header, body);
        stream.write_all(&request)?;
        let mut parser = ResponseParser::default();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                return parser.finish_eof();
            }
            if let Some(resp) = parser.push(&chunk[..n])? {
                return Ok(resp);
            }
        }
    }
}

/// Serialize an HTTP/1.1 request. Bodies are always JSON here.
pub fn build_request(method: &str, path: &str, host: &str, body: Option<&[u8]>) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + body.map_or(0, <[u8]>::len));
    out.extend_from_slice(format!("{method} {path} HTTP/1.1\r\n").as_bytes());
    out.extend_from_slice(format!("host: {host}\r\n").as_bytes());
    out.extend_from_slice(b"user-agent: tachyon-serverless-sdk\r\n");
    out.extend_from_slice(b"accept: application/json\r\n");
    out.extend_from_slice(b"connection: close\r\n");
    match body {
        Some(b) => {
            out.extend_from_slice(b"content-type: application/json\r\n");
            out.extend_from_slice(format!("content-length: {}\r\n\r\n", b.len()).as_bytes());
            out.extend_from_slice(b);
        }
        None => out.extend_from_slice(b"content-length: 0\r\n\r\n"),
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyMode {
    Length(usize),
    Chunked,
    UntilClose,
}

#[derive(Debug)]
struct Head {
    status: u16,
    headers: Vec<(String, String)>,
    body_start: usize,
    mode: BodyMode,
}

/// Incremental HTTP/1.1 response parser. Feed bytes with [`push`]; it returns
/// the response once the body is complete. Call [`finish_eof`] at end of
/// stream for read-until-close bodies.
///
/// [`push`]: ResponseParser::push
/// [`finish_eof`]: ResponseParser::finish_eof
#[derive(Debug, Default)]
pub struct ResponseParser {
    buf: Vec<u8>,
    head: Option<Head>,
}

impl ResponseParser {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Option<HttpResponse>, SdkError> {
        self.buf.extend_from_slice(bytes);
        if self.head.is_none() {
            let Some(head) = parse_head(&self.buf)? else {
                return Ok(None);
            };
            self.head = Some(head);
        }
        let head = self.head.as_ref().expect("head parsed");
        let body = &self.buf[head.body_start..];
        match head.mode {
            BodyMode::Length(n) => {
                if body.len() >= n {
                    Ok(Some(self.complete(body[..n].to_vec())))
                } else {
                    Ok(None)
                }
            }
            BodyMode::Chunked => match decode_chunked(body)? {
                Some(decoded) => Ok(Some(self.complete(decoded))),
                None => Ok(None),
            },
            BodyMode::UntilClose => Ok(None),
        }
    }

    pub fn finish_eof(self) -> Result<HttpResponse, SdkError> {
        let Some(head) = self.head else {
            return Err(SdkError::Protocol(
                "connection closed before response head".into(),
            ));
        };
        let body = self.buf[head.body_start..].to_vec();
        match head.mode {
            BodyMode::UntilClose => Ok(HttpResponse {
                status: head.status,
                headers: head.headers,
                body,
            }),
            BodyMode::Length(n) => Err(SdkError::Protocol(format!(
                "connection closed with incomplete body ({} of {n} bytes)",
                body.len()
            ))),
            BodyMode::Chunked => Err(SdkError::Protocol(
                "connection closed inside chunked body".into(),
            )),
        }
    }

    fn complete(&mut self, body: Vec<u8>) -> HttpResponse {
        let head = self.head.take().expect("head parsed");
        HttpResponse {
            status: head.status,
            headers: head.headers,
            body,
        }
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn parse_head(buf: &[u8]) -> Result<Option<Head>, SdkError> {
    let Some(body_start) = find_head_end(buf) else {
        if buf.len() > 64 * 1024 {
            return Err(SdkError::Protocol("response head too large".into()));
        }
        return Ok(None);
    };
    let head = std::str::from_utf8(&buf[..body_start - 4])
        .map_err(|_| SdkError::Protocol("response head is not utf-8".into()))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let mut parts = status_line.splitn(3, ' ');
    let version = parts.next().unwrap_or("");
    if !version.starts_with("HTTP/1.") {
        return Err(SdkError::Protocol(format!(
            "bad status line: {status_line:?}"
        )));
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| SdkError::Protocol(format!("bad status line: {status_line:?}")))?;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once(':') else {
            return Err(SdkError::Protocol(format!("bad header line: {line:?}")));
        };
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
    }
    let mut mode = BodyMode::UntilClose;
    if let Some((_, te)) = headers.iter().find(|(k, _)| k == "transfer-encoding") {
        if te.to_ascii_lowercase().contains("chunked") {
            mode = BodyMode::Chunked;
        }
    } else if let Some((_, cl)) = headers.iter().find(|(k, _)| k == "content-length") {
        let n: usize = cl
            .parse()
            .map_err(|_| SdkError::Protocol(format!("bad content-length: {cl:?}")))?;
        mode = BodyMode::Length(n);
    }
    // 204 / 304 never carry a body.
    if status == 204 || status == 304 {
        mode = BodyMode::Length(0);
    }
    Ok(Some(Head {
        status,
        headers,
        body_start,
        mode,
    }))
}

/// Decode a chunked body. Returns `None` while the terminating chunk has not
/// arrived yet.
fn decode_chunked(mut buf: &[u8]) -> Result<Option<Vec<u8>>, SdkError> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = buf.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let size_line = std::str::from_utf8(&buf[..line_end])
            .map_err(|_| SdkError::Protocol("bad chunk size line".into()))?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| SdkError::Protocol(format!("bad chunk size: {size_hex:?}")))?;
        buf = &buf[line_end + 2..];
        if size == 0 {
            // Trailers (if any) end with an empty line.
            return if buf.windows(2).any(|w| w == b"\r\n") {
                Ok(Some(out))
            } else {
                Ok(None)
            };
        }
        if buf.len() < size + 2 {
            return Ok(None);
        }
        out.extend_from_slice(&buf[..size]);
        buf = &buf[size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_request_with_body() {
        let r = build_request("POST", "/x", "127.0.0.1:1", Some(b"{}"));
        let s = String::from_utf8(r).unwrap();
        assert!(s.starts_with("POST /x HTTP/1.1\r\nhost: 127.0.0.1:1\r\n"));
        assert!(s.contains("content-length: 2\r\n\r\n{}"));
        assert!(s.contains("connection: close"));
    }

    #[test]
    fn parses_content_length_incrementally() {
        let mut p = ResponseParser::default();
        assert!(
            p.push(b"HTTP/1.1 200 OK\r\ncontent-type: app")
                .unwrap()
                .is_none()
        );
        assert!(
            p.push(b"lication/json\r\nContent-Length: 5\r\n\r\nhel")
                .unwrap()
                .is_none()
        );
        let r = p.push(b"lo").unwrap().unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.header("Content-Type"), Some("application/json"));
        assert_eq!(r.body, b"hello");
    }

    #[test]
    fn parses_chunked() {
        let mut p = ResponseParser::default();
        let raw = b"HTTP/1.1 202 Accepted\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
        let r = p.push(raw).unwrap().unwrap();
        assert_eq!(r.status, 202);
        assert_eq!(r.body, b"abcde");
    }

    #[test]
    fn until_close_needs_eof() {
        let mut p = ResponseParser::default();
        assert!(p.push(b"HTTP/1.1 410 Gone\r\n\r\nbye").unwrap().is_none());
        let r = p.finish_eof().unwrap();
        assert_eq!(r.status, 410);
        assert_eq!(r.body, b"bye");
    }

    #[test]
    fn incomplete_length_body_at_eof_is_error() {
        let mut p = ResponseParser::default();
        assert!(
            p.push(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nabc")
                .unwrap()
                .is_none()
        );
        assert!(p.finish_eof().is_err());
    }

    #[test]
    fn url_parsing() {
        let c = RuntimeClient::new("http://127.0.0.1:9001").unwrap();
        assert_eq!(c.addr().port(), 9001);
        assert!(RuntimeClient::new("https://127.0.0.1:1").is_err());
        assert!(RuntimeClient::new("http://").is_err());
        let c = RuntimeClient::new("http://127.0.0.1:9001/").unwrap();
        assert_eq!(c.addr().port(), 9001);
    }
}
