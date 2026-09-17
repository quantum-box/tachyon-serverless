//! PLT-4622 network policy probes: what a guest with a policed network device
//! can and cannot reach.
//!
//! - `{"probe":"net", ...}` runs every listed check concurrently and reports
//!   each outcome; the measurement script decides which outcomes are expected
//!   for the egress profile under test:
//!   - `connect`: `address:port` TCP targets (IPv4 or `[IPv6]:port`);
//!   - `udp_dns`: `server:53` targets, each sent one raw DNS `A` query for
//!     `dns_name` over UDP (so a resolver other than the configured one can be
//!     asked directly, bypassing `/etc/resolv.conf`);
//!   - `resolve`: names resolved through the system resolver; every returned
//!     address is then connected to on `resolve_port` (DNS answers that point
//!     at a denied address, e.g. `169.254.169.254.nip.io`);
//!   - `http_redirect`: an `http://` URL fetched with one GET; the `Location`
//!     of a 3xx answer is then connected to (an HTTP redirect towards a
//!     denied address).
//! - `{"probe":"listen","port":N,"duration_ms":M}` accepts TCP connections on
//!   `0.0.0.0:N` for M ms and reports every peer that got through (the other
//!   side of the cross-tenant check).

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const MAX_LISTEN_MS: u64 = 120_000;
const HTTP_READ_LIMIT: usize = 16 * 1024;

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn strings(payload: &Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn ms(payload: &Value, key: &str, default: u64) -> Duration {
    Duration::from_millis(
        payload
            .get(key)
            .and_then(Value::as_u64)
            .unwrap_or(default)
            .clamp(1, 60_000),
    )
}

fn io_kind(e: &std::io::Error) -> Value {
    json!({"error_kind": format!("{:?}", e.kind()), "os_error": e.raw_os_error(), "error": e.to_string()})
}

/// One TCP connect to a parsed socket address.
pub fn tcp_connect(target: &str, timeout: Duration) -> Value {
    let started = Instant::now();
    let addr: SocketAddr = match target.parse() {
        Ok(a) => a,
        Err(e) => {
            return json!({"target": target, "attempted": false, "connected": false,
                "error_kind": "InvalidTarget", "error": e.to_string(), "elapsed_ms": 0});
        }
    };
    match TcpStream::connect_timeout(&addr, timeout) {
        Ok(stream) => json!({
            "target": target, "attempted": true, "connected": true,
            "local": stream.local_addr().ok().map(|a| a.to_string()),
            "peer": stream.peer_addr().ok().map(|a| a.to_string()),
            "elapsed_ms": elapsed_ms(started),
        }),
        Err(e) => {
            let mut v = json!({"target": target, "attempted": true, "connected": false,
                "elapsed_ms": elapsed_ms(started)});
            merge(&mut v, io_kind(&e));
            v
        }
    }
}

fn merge(into: &mut Value, from: Value) {
    if let (Value::Object(a), Value::Object(b)) = (into, from) {
        a.extend(b);
    }
}

/// A DNS query for `name`, type A, class IN, recursion desired.
pub fn dns_query(id: u16, name: &str) -> Vec<u8> {
    let mut q = Vec::with_capacity(32 + name.len());
    q.extend_from_slice(&id.to_be_bytes());
    q.extend_from_slice(&[0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    for label in name.trim_end_matches('.').split('.') {
        let bytes = &label.as_bytes()[..label.len().min(63)];
        q.push(bytes.len() as u8);
        q.extend_from_slice(bytes);
    }
    q.extend_from_slice(&[0x00, 0x00, 0x01, 0x00, 0x01]);
    q
}

fn skip_name(msg: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        let len = *msg.get(pos)? as usize;
        if len == 0 {
            return Some(pos + 1);
        }
        if len & 0xC0 == 0xC0 {
            return Some(pos + 2);
        }
        pos += 1 + len;
    }
}

/// IPv4 addresses in the answer section of a DNS response to `id`.
pub fn dns_answers(id: u16, msg: &[u8]) -> Option<Vec<Ipv4Addr>> {
    if msg.len() < 12 || msg[0..2] != id.to_be_bytes() || msg[2] & 0x80 == 0 {
        return None;
    }
    let qd = u16::from_be_bytes([msg[4], msg[5]]) as usize;
    let an = u16::from_be_bytes([msg[6], msg[7]]) as usize;
    let mut pos = 12;
    for _ in 0..qd {
        pos = skip_name(msg, pos)? + 4;
    }
    let mut out = Vec::new();
    for _ in 0..an {
        pos = skip_name(msg, pos)?;
        let header = msg.get(pos..pos + 10)?;
        let rtype = u16::from_be_bytes([header[0], header[1]]);
        let rdlen = u16::from_be_bytes([header[8], header[9]]) as usize;
        let data = msg.get(pos + 10..pos + 10 + rdlen)?;
        if rtype == 1 && rdlen == 4 {
            out.push(Ipv4Addr::new(data[0], data[1], data[2], data[3]));
        }
        pos += 10 + rdlen;
    }
    Some(out)
}

/// One raw UDP DNS query to `server` (e.g. `8.8.8.8:53`).
pub fn udp_dns(server: &str, name: &str, timeout: Duration) -> Value {
    let started = Instant::now();
    let id: u16 = (std::process::id() as u16) ^ 0x5a5a;
    let result = (|| -> std::io::Result<Option<Vec<Ipv4Addr>>> {
        let addr: SocketAddr = server
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let sock = UdpSocket::bind("0.0.0.0:0")?;
        sock.set_read_timeout(Some(timeout))?;
        sock.send_to(&dns_query(id, name), addr)?;
        let mut buf = [0u8; 1500];
        let deadline = Instant::now() + timeout;
        loop {
            let (n, from) = sock.recv_from(&mut buf)?;
            if from == addr
                && let Some(answers) = dns_answers(id, &buf[..n])
            {
                return Ok(Some(answers));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
        }
    })();
    match result {
        Ok(Some(answers)) => json!({"server": server, "name": name, "answered": true,
            "addresses": answers.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            "elapsed_ms": elapsed_ms(started)}),
        Ok(None) => json!({"server": server, "name": name, "answered": false,
            "error_kind": "NoMatchingAnswer", "elapsed_ms": elapsed_ms(started)}),
        Err(e) => {
            let mut v = json!({"server": server, "name": name, "answered": false,
                "elapsed_ms": elapsed_ms(started)});
            merge(&mut v, io_kind(&e));
            v
        }
    }
}

/// Resolve `name` through the system resolver (bounded), then connect to every
/// address on `port`.
pub fn resolve_and_connect(
    name: &str,
    port: u16,
    dns_timeout: Duration,
    timeout: Duration,
) -> Value {
    let started = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    let query = format!("{name}:{port}");
    std::thread::spawn(move || {
        let _ = tx.send(
            query
                .to_socket_addrs()
                .map(|a| a.collect::<Vec<_>>())
                .map_err(|e| io_kind(&e)),
        );
    });
    match rx.recv_timeout(dns_timeout) {
        Ok(Ok(addrs)) => {
            let connects: Vec<Value> = addrs
                .iter()
                .map(|a| tcp_connect(&a.to_string(), timeout))
                .collect();
            json!({"name": name, "resolved": !addrs.is_empty(),
                "addresses": addrs.iter().map(|a| a.ip().to_string()).collect::<Vec<_>>(),
                "dns_elapsed_ms": elapsed_ms(started),
                "connects": connects,
                "connected": connects.iter().any(|c| c["connected"] == true)})
        }
        Ok(Err(err)) => {
            let mut v = json!({"name": name, "resolved": false, "connected": false,
                "dns_elapsed_ms": elapsed_ms(started)});
            merge(&mut v, err);
            v
        }
        Err(_) => json!({"name": name, "resolved": false, "connected": false,
            "error_kind": "ProbeTimeout", "dns_elapsed_ms": elapsed_ms(started)}),
    }
}

/// `http://host[:port]/path` -> (host, port, path).
pub fn parse_http_url(url: &str) -> Option<(String, u16, String)> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_owned()),
        None => (rest, "/".to_owned()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => {
            (h.trim_matches(['[', ']']).to_owned(), p.parse().ok()?)
        }
        _ => (authority.to_owned(), 80),
    };
    (!host.is_empty()).then_some((host, port, path))
}

/// GET `url` once; for a 3xx answer, connect to its `Location`.
pub fn http_redirect(url: &str, dns_timeout: Duration, timeout: Duration) -> Value {
    let started = Instant::now();
    let Some((host, port, path)) = parse_http_url(url) else {
        return json!({"url": url, "fetched": false, "error_kind": "InvalidUrl"});
    };
    let first = resolve_and_connect(&host, port, dns_timeout, timeout);
    let Some(peer) = first["connects"]
        .as_array()
        .and_then(|c| c.iter().find(|c| c["connected"] == true))
        .and_then(|c| c["peer"].as_str())
        .and_then(|p| p.parse::<SocketAddr>().ok())
    else {
        return json!({"url": url, "fetched": false, "first": first,
            "error_kind": "FirstHopUnreachable", "elapsed_ms": elapsed_ms(started)});
    };
    let response = (|| -> std::io::Result<String> {
        let mut s = TcpStream::connect_timeout(&peer, timeout)?;
        s.set_read_timeout(Some(timeout))?;
        write!(
            s,
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: tachyon-isolation-probe\r\nConnection: close\r\n\r\n"
        )?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        while buf.len() < HTTP_READ_LIMIT {
            let n = s.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    })();
    let head = match response {
        Ok(h) => h,
        Err(e) => {
            let mut v = json!({"url": url, "fetched": false, "elapsed_ms": elapsed_ms(started)});
            merge(&mut v, io_kind(&e));
            return v;
        }
    };
    let status: Option<u16> = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok());
    let location = head
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("location"))
        })
        .map(|(_, v)| v.trim().to_owned());
    let follow = location
        .as_deref()
        .and_then(parse_http_url)
        .map(|(h, p, _)| {
            let target = if h.contains(':') {
                format!("[{h}]:{p}")
            } else {
                format!("{h}:{p}")
            };
            if target.parse::<SocketAddr>().is_ok() {
                tcp_connect(&target, timeout)
            } else {
                resolve_and_connect(&h, p, dns_timeout, timeout)
            }
        });
    json!({"url": url, "fetched": true, "status": status, "location": location,
        "follow": follow, "follow_connected": follow.as_ref().is_some_and(|f| f["connected"] == true),
        "elapsed_ms": elapsed_ms(started)})
}

fn run_parallel<T: Send + 'static>(jobs: Vec<Box<dyn FnOnce() -> T + Send>>) -> Vec<T> {
    let handles: Vec<_> = jobs.into_iter().map(std::thread::spawn).collect();
    handles.into_iter().filter_map(|h| h.join().ok()).collect()
}

/// `{"probe":"net"}`.
pub fn net_report(payload: &Value) -> Value {
    let timeout = ms(payload, "connect_timeout_ms", 2_000);
    let dns_timeout = ms(payload, "dns_timeout_ms", 5_000);
    let dns_name = payload
        .get("dns_name")
        .and_then(Value::as_str)
        .unwrap_or("example.com")
        .to_owned();
    let resolve_port = payload
        .get("resolve_port")
        .and_then(Value::as_u64)
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or(80);

    type Job = Box<dyn FnOnce() -> (String, Value) + Send>;
    let mut jobs: Vec<Job> = Vec::new();
    for t in strings(payload, "connect") {
        jobs.push(Box::new(move || {
            ("connect".into(), tcp_connect(&t, timeout))
        }));
    }
    for s in strings(payload, "udp_dns") {
        let n = dns_name.clone();
        jobs.push(Box::new(move || {
            ("udp_dns".into(), udp_dns(&s, &n, dns_timeout))
        }));
    }
    for n in strings(payload, "resolve") {
        jobs.push(Box::new(move || {
            (
                "resolve".into(),
                resolve_and_connect(&n, resolve_port, dns_timeout, timeout),
            )
        }));
    }
    if let Some(url) = payload.get("http_redirect").and_then(Value::as_str) {
        let url = url.to_owned();
        jobs.push(Box::new(move || {
            (
                "http_redirect".into(),
                http_redirect(&url, dns_timeout, timeout),
            )
        }));
    }
    let started = Instant::now();
    let results = run_parallel(jobs);
    let pick = |kind: &str| -> Vec<Value> {
        results
            .iter()
            .filter(|(k, _)| k == kind)
            .map(|(_, v)| v.clone())
            .collect()
    };
    json!({
        "connect": pick("connect"),
        "udp_dns": pick("udp_dns"),
        "resolve": pick("resolve"),
        "http_redirect": pick("http_redirect").into_iter().next(),
        "resolv_conf": std::fs::read_to_string("/etc/resolv.conf").ok(),
        "proc_net_pnp": std::fs::read_to_string("/proc/net/pnp").ok(),
        "ipv6_addresses": std::fs::read_to_string("/proc/net/if_inet6").ok(),
        "elapsed_ms": elapsed_ms(started),
    })
}

/// `{"probe":"listen"}`.
pub fn listen_report(payload: &Value) -> Value {
    let port = payload
        .get("port")
        .and_then(Value::as_u64)
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or(8080);
    let duration = Duration::from_millis(
        payload
            .get("duration_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000)
            .min(MAX_LISTEN_MS),
    );
    let started = Instant::now();
    let listener = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(l) => l,
        Err(e) => {
            let mut v = json!({"port": port, "listening": false});
            merge(&mut v, io_kind(&e));
            return v;
        }
    };
    let _ = listener.set_nonblocking(true);
    let mut peers = Vec::new();
    while started.elapsed() < duration {
        match listener.accept() {
            Ok((_, peer)) => peers.push(peer.to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    json!({"port": port, "listening": true, "duration_ms": elapsed_ms(started),
        "accepted": peers.len(), "peers": peers})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_query_and_answer_roundtrip() {
        let q = dns_query(0x1234, "example.com");
        assert_eq!(&q[..2], &[0x12, 0x34]);
        assert_eq!(&q[12..25], b"\x07example\x03com\x00");
        // Response: header (QR set, 1 question, 1 answer), question, answer with
        // a compression pointer to the question name.
        let mut r = vec![0x12, 0x34, 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0];
        r.extend_from_slice(&q[12..]);
        r.extend_from_slice(&[0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 93, 184, 216, 34]);
        assert_eq!(
            dns_answers(0x1234, &r),
            Some(vec![Ipv4Addr::new(93, 184, 216, 34)])
        );
        assert_eq!(dns_answers(0x9999, &r), None, "wrong id");
        assert_eq!(dns_answers(0x1234, &q), None, "a query is not an answer");
        assert_eq!(dns_answers(0x1234, &r[..20]), None, "truncated");
    }

    #[test]
    fn http_urls_parse() {
        assert_eq!(
            parse_http_url("http://httpbin.org/redirect-to?url=x"),
            Some(("httpbin.org".into(), 80, "/redirect-to?url=x".into()))
        );
        assert_eq!(
            parse_http_url("http://169.254.169.254:8080"),
            Some(("169.254.169.254".into(), 8080, "/".into()))
        );
        assert_eq!(
            parse_http_url("http://[fe80::1]:80/"),
            Some(("fe80::1".into(), 80, "/".into()))
        );
        assert_eq!(parse_http_url("https://example.com/"), None);
    }

    #[test]
    fn unreachable_targets_are_reported_not_panicked() {
        let v = tcp_connect("not-an-address", Duration::from_millis(10));
        assert_eq!(v["attempted"], false);
        let v = udp_dns("nope", "example.com", Duration::from_millis(10));
        assert_eq!(v["answered"], false);
        let report =
            net_report(&json!({"connect": ["bad"], "udp_dns": [], "connect_timeout_ms": 10}));
        assert_eq!(report["connect"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn listen_reports_accepted_peers() {
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let handle =
            std::thread::spawn(move || listen_report(&json!({"port": port, "duration_ms": 600})));
        std::thread::sleep(Duration::from_millis(150));
        let _ = TcpStream::connect(("127.0.0.1", port));
        let v = handle.join().unwrap();
        assert_eq!(v["listening"], true);
        assert!(v["accepted"].as_u64().unwrap() >= 1, "{v}");
    }
}
