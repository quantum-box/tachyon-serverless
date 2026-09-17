//! Host connection: vsock (Firecracker guest) or a unix socket (process
//! provider). Both yield a plain duplex byte stream; framing lives in the
//! protocol crate.

use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

/// Duplex byte stream to the host.
pub trait HostStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> HostStream for T {}

pub type BoxedHostStream = Box<dyn HostStream>;

/// Connect to a unix socket, retrying briefly while the host is still binding.
pub async fn connect_unix(path: &Path) -> std::io::Result<BoxedHostStream> {
    const ATTEMPTS: u32 = 30;
    let mut last_err = None;
    for attempt in 0..ATTEMPTS {
        match tokio::net::UnixStream::connect(path).await {
            Ok(stream) => return Ok(Box::new(stream)),
            Err(e) => {
                let retry = matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                );
                last_err = Some(e);
                if !retry || attempt + 1 == ATTEMPTS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.expect("at least one attempt"))
}

/// Connect to the host over vsock (`cid`, `port`). Firecracker guests use
/// CID 2 (the host) and the port the host listens on (`<uds>_<port>`).
#[cfg(target_os = "linux")]
pub async fn connect_vsock(cid: u32, port: u32) -> std::io::Result<BoxedHostStream> {
    const ATTEMPTS: u32 = 30;
    let addr = tokio_vsock::VsockAddr::new(cid, port);
    let mut last_err = None;
    for attempt in 0..ATTEMPTS {
        match tokio_vsock::VsockStream::connect(addr).await {
            Ok(stream) => return Ok(Box::new(stream)),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 == ATTEMPTS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.expect("at least one attempt"))
}

/// One vsock connection attempt (the restore link retries itself).
#[cfg(target_os = "linux")]
pub async fn connect_vsock_once(cid: u32, port: u32) -> std::io::Result<BoxedHostStream> {
    let s = tokio_vsock::VsockStream::connect(tokio_vsock::VsockAddr::new(cid, port)).await?;
    Ok(Box::new(s))
}

#[cfg(not(target_os = "linux"))]
pub async fn connect_vsock_once(cid: u32, port: u32) -> std::io::Result<BoxedHostStream> {
    connect_vsock(cid, port).await
}

#[cfg(not(target_os = "linux"))]
pub async fn connect_vsock(_cid: u32, _port: u32) -> std::io::Result<BoxedHostStream> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "vsock transport is only available on Linux guests; use --transport unix on this platform",
    ))
}
